use backon::ExponentialBuilder;
use backon::Retryable;
use futures::Stream;
use http::header::{ACCEPT_ENCODING, USER_AGENT};
use http::StatusCode;
use iterable::*;
use reqwest::header::HeaderValue;
use reqwest::{RequestBuilder, Response, Url};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::RwLock;
use tracing::*;

use crate::auth::Auth;
use crate::build_dataset;
use crate::error::{Error, Result};
use crate::header::*;
use crate::models::Column;
#[cfg(feature = "spooling")]
use crate::models::SpooledData;
use crate::pager::QueryPager;
use crate::retry::RetryPolicy;
use crate::selected_role::SelectedRole;
use crate::session::{Session, SessionSnapshot};
#[cfg(feature = "spooling")]
use crate::spooling::decompress_segment_bytes;
#[cfg(feature = "spooling")]
use crate::spooling::{SegmentFetcher, SpoolingEncoding};
use crate::transaction::TransactionId;
use crate::{DataSet, QueryResult, Row, Trino};

mod builder;

pub use builder::ClientBuilder;

// TODO:
// allow_redirects

/// A configured Trino client.
///
/// Created with [`ClientBuilder`]. Cheap to share: it wraps a connection-pooled
/// HTTP client, so build one and reuse it for all queries. The main entry
/// points are [`get_all`](Client::get_all) (buffer the result),
/// [`stream`](Client::stream) (stream it lazily) and [`execute`](Client::execute)
/// (run a statement).
pub struct Client {
    client: reqwest::Client,
    session: RwLock<Session>,
    auth: Option<Auth>,
    retry: RetryPolicy,
    url: Url,
    #[cfg(feature = "spooling")]
    segment_fetcher: SegmentFetcher,
}

/// Outcome of a statement run with [`Client::execute`].
#[derive(Debug)]
pub struct ExecuteResult {
    /// URI of the output, when the statement produces one.
    pub output_uri: Option<String>,
    /// The kind of update (e.g. `INSERT`, `CREATE TABLE`), if reported.
    pub update_type: Option<String>,
    /// Number of rows affected, if reported.
    pub update_count: Option<u64>,
}

fn add_prepare_header(mut builder: RequestBuilder, session: &Session) -> RequestBuilder {
    // Validated at build time not to collide with any header set below.
    for (k, v) in &session.extra_headers {
        builder = builder.header(k, v);
    }
    // The session user is who the statement runs as. Authentication still uses
    // the connection principal, so the two differing is impersonation, which
    // the coordinator only permits if that principal holds `impersonate`.
    //
    // With neither set the header is omitted, and Trino takes the user from the
    // authenticated identity instead. That is the correct shape for interactive
    // OAuth 2.0: the identity provider decides who you are, and sending a name
    // that disagrees with it is read as an impersonation attempt rather than
    // ignored -- so a header we invented would be refused for the user's own
    // account.
    if let Some(user) = session.session_user.as_ref().or(session.user.as_ref()) {
        builder = builder.header(HEADER_USER, user);
    }
    // TODO: difference with session.source?
    builder = builder.header(USER_AGENT, "trino-rust-client");
    if session.compression_disabled {
        builder = builder.header(ACCEPT_ENCODING, "identity")
    }
    builder
}

/// Capabilities the client always advertises. Both are load-bearing — the type
/// decoder relies on `PARAMETRIC_DATETIME` and the session path on `PATH` — so
/// callers can only add to this set, never replace it.
const BUILTIN_CLIENT_CAPABILITIES: [&str; 2] = ["PARAMETRIC_DATETIME", "PATH"];

/// The `X-Trino-Client-Capabilities` value: the built-ins unioned with any
/// configured extras, sorted so the header is deterministic.
fn client_capabilities_value(session: &Session) -> String {
    let mut caps: Vec<&str> = BUILTIN_CLIENT_CAPABILITIES.to_vec();
    caps.extend(session.client_capabilities.iter().map(String::as_str));
    caps.sort_unstable();
    caps.dedup();
    caps.join(",")
}

fn add_session_header(mut builder: RequestBuilder, session: &Session) -> RequestBuilder {
    builder = add_prepare_header(builder, session);
    builder = builder.header(HEADER_SOURCE, &session.source);

    if let Some(v) = &session.trace_token {
        builder = builder.header(HEADER_TRACE_TOKEN, v);
    }

    if !session.client_tags.is_empty() {
        builder = builder.header(HEADER_CLIENT_TAGS, session.client_tags.by_ref().join(","));
    }

    if let Some(v) = &session.client_info {
        builder = builder.header(HEADER_CLIENT_INFO, v);
    }

    if let Some(v) = &session.catalog {
        builder = builder.header(HEADER_CATALOG, v);
    }

    if let Some(v) = &session.schema {
        builder = builder.header(HEADER_SCHEMA, v);
    }

    if let Some(v) = &session.path {
        builder = builder.header(HEADER_PATH, v);
    }
    if let Some(v) = &session.timezone {
        builder = builder.header(HEADER_TIME_ZONE, v.to_string())
    }
    if let Some(v) = &session.locale {
        builder = builder.header(HEADER_LANGUAGE, v);
    }
    builder = add_header_map(builder, HEADER_SESSION, &session.properties);
    builder = add_header_map(
        builder,
        HEADER_RESOURCE_ESTIMATE,
        &session.resource_estimates,
    );
    builder = add_header_map(
        builder,
        HEADER_ROLE,
        &session
            .roles
            .by_ref()
            .map_kv(|(k, v)| (k.to_string(), v.to_string())),
    );
    builder = add_header_map(builder, HEADER_EXTRA_CREDENTIAL, &session.extra_credentials);
    builder = add_header_map(
        builder,
        HEADER_PREPARED_STATEMENT,
        &session.prepared_statements,
    );
    builder = builder.header(HEADER_TRANSACTION, session.transaction_id.as_header_value());
    builder = builder.header(
        HEADER_CLIENT_CAPABILITIES,
        client_capabilities_value(session),
    );

    // Advertise the spooling protocol only when explicitly opted in.
    #[cfg(feature = "spooling")]
    {
        if let Some(encoding) = session.spooling_encoding {
            builder = builder.header(HEADER_SPOOLING, encoding.as_str());
        }
    }

    builder
}

fn add_header_map<'a>(
    mut builder: RequestBuilder,
    header: &str,
    map: impl IntoIterator<Item = (&'a String, &'a String)>,
) -> RequestBuilder {
    for (k, v) in map {
        let kv = encode_kv(k, v);
        builder = builder.header(header, kv);
    }
    builder
}

macro_rules! set_header {
    ($session:expr, $header:expr, $resp:expr) => {
        set_header!($session, $header, $resp, |x: &str| Some(Some(
            x.to_string()
        )));
    };

    ($session:expr, $header:expr, $resp:expr, $from_str:expr) => {
        if let Some(v) = $resp.headers().get($header) {
            match v.to_str() {
                Ok(s) => {
                    if let Some(s) = $from_str(s) {
                        $session = s;
                    }
                }
                Err(e) => warn!("parse header {} failed, reason: {}", $header, e),
            }
        }
    };
}

macro_rules! clear_header {
    ($session:expr, $header:expr, $resp:expr) => {
        if let Some(_) = $resp.headers().get($header) {
            $session = Default::default();
        }
    };
}

macro_rules! set_header_map {
    ($session:expr, $header:expr, $resp:expr) => {
        set_header_map!($session, $header, $resp, |x: &str| Some(x.to_string()));
    };
    ($session:expr, $header:expr, $resp:expr, $from_str:expr) => {
        for v in $resp.headers().get_all($header) {
            if let Some((k, v)) = decode_kv_from_header(v) {
                if let Some(parsed) = $from_str(&v) {
                    $session.insert(k, parsed);
                } else {
                    warn!("parse header {} value '{}' failed, ignoring", $header, v)
                }
            } else {
                warn!("decode '{:?}' failed", v)
            }
        }
    };
}

macro_rules! clear_header_map {
    ($session:expr, $header:expr, $resp:expr) => {
        for v in $resp.headers().get_all($header) {
            match v.to_str() {
                Ok(s) => {
                    $session.remove(s);
                }
                Err(e) => warn!("parse header {} failed, reason: {}", $header, e),
            }
        }
    };
}

fn transient_status(code: &StatusCode) -> bool {
    matches!(
        *code,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

/// Retry predicate for **idempotent** requests (fetching result pages via
/// `GET nextUri`). Any transient failure is safe to retry: gateway/availability
/// responses (HTTP 502/503/504) and low-level connect/timeout errors. Query,
/// decode, protocol and other errors are terminal.
pub(crate) fn need_retry_fetch(e: &Error) -> bool {
    match e {
        Error::HttpError(e) => {
            e.is_timeout() || e.is_connect() || e.status().as_ref().is_some_and(transient_status)
        }
        Error::HttpNotOk(code, _) => transient_status(code),
        _ => false,
    }
}

/// Retry predicate for **query submission** (`POST /v1/statement`). Only retry
/// when the request was definitely NOT processed by the server, so a
/// non-idempotent statement (e.g. `INSERT`/`UPDATE`/DDL via [`Client::execute`])
/// is never submitted twice. A timeout — or a 502/504 from an intermediary — is
/// ambiguous (the query may already be running) and is treated as terminal.
fn need_retry_submit(e: &Error) -> bool {
    match e {
        // Connection was never established, so the request was not sent.
        Error::HttpError(e) => e.is_connect(),
        // 503 means the coordinator rejected the request without processing it.
        Error::HttpNotOk(code, _) => *code == StatusCode::SERVICE_UNAVAILABLE,
        _ => false,
    }
}

/// Everything needed to fire a best-effort query cancellation from
/// [`RowStream`]'s `Drop`, without borrowing the [`Client`].
pub(crate) struct CancelOnDrop {
    pub(crate) client: reqwest::Client,
    pub(crate) url: String,
    pub(crate) auth: Option<Auth>,
}

impl CancelOnDrop {
    /// The cancellation `DELETE`, with authentication applied.
    ///
    /// This runs detached from the [`Client`], so unlike [`Client::auth_req`] +
    /// [`Client::send`] it can only use an already-cached OAuth2 token — a
    /// cancellation must never trigger an interactive login. With no cached
    /// token, or an expired one, the coordinator rejects the `DELETE` and times
    /// the query out on its own instead.
    pub(crate) fn authed_req(&self) -> RequestBuilder {
        let req = self.client.delete(&self.url);
        match self.auth.as_ref() {
            Some(Auth::Basic(u, p)) => req.basic_auth(u, p.as_ref()),
            Some(Auth::Jwt(t)) => req.bearer_auth(t),
            Some(Auth::OAuth2(state)) => match state.cached_token() {
                Some(t) => req.bearer_auth(t),
                None => req,
            },
            _ => req,
        }
    }

    /// Best-effort `DELETE` of the query. Requires an active Tokio runtime;
    /// silently does nothing if there is none (e.g. drop outside a runtime).
    pub(crate) fn spawn_fire(self) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = self.authed_req().send().await;
            });
        }
    }
}

/// A lazy stream of query rows, with the result columns resolved up front.
///
/// Created by [`Client::stream`]. The result columns are available immediately
/// via [`RowStream::columns`]; rows are then produced lazily, page by page, by
/// the [`Stream`] implementation — the whole result set is never buffered in
/// memory.
///
/// `RowStream` is [`Unpin`], so it can be polled directly (e.g. with
/// [`StreamExt::next`](futures::StreamExt::next)) without `pin!`, and [`Send`],
/// so it can be held across `.await` inside a spawned task.
///
/// # Cancellation
/// Dropping a `RowStream` before it is exhausted best-effort cancels the query
/// on the Trino coordinator (a fire-and-forget `DELETE`), so early termination
/// (`take`, `break`, an error, a dropped task) does not leave the query running
/// server-side and holding coordinator resources. Cancellation is skipped once
/// the query has finished normally, and requires a Tokio runtime to be active
/// at drop time.
pub struct RowStream<'a, T> {
    columns: Vec<Column>,
    query_id: String,
    cancel: Option<CancelOnDrop>,
    /// Used by `cancel` so an explicit cancellation goes through the same
    /// authenticated, retried path as `Client::cancel`. `Drop` cannot: it must
    /// not block on an interactive login, so it keeps using `CancelOnDrop`.
    client: &'a Client,
    // Entered on every poll so events emitted while streaming (page fetches,
    // segment downloads) carry the query_id — the span from `stream()` itself
    // would otherwise close as soon as the RowStream is handed back.
    span: tracing::Span,
    inner: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'a>>,
}

impl<T> RowStream<'_, T> {
    /// The result columns (name, Trino type name and full type signature),
    /// resolved before the first row is produced.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// The coordinator's query id, usable with [`Client::cancel`].
    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    /// Cancel the query on the coordinator and report whether it succeeded.
    ///
    /// Dropping a `RowStream` cancels best-effort without reporting; use this
    /// when the outcome matters. Unlike `Drop`, this goes through the client's
    /// normal request path, so it retries under the [`RetryPolicy`] and
    /// re-authenticates if an OAuth2 token has expired.
    pub async fn cancel(mut self) -> Result<()> {
        // Disarm Drop so it does not fire a second, redundant DELETE.
        if self.cancel.take().is_none() {
            // Already finished normally — nothing to cancel.
            return Ok(());
        }
        self.client.cancel(&self.query_id).await
    }
}

impl<T> Stream for RowStream<'_, T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        let _enter = me.span.enter();
        let polled = me.inner.as_mut().poll_next(cx);
        if let Poll::Ready(None) = polled {
            // The query finished normally — there is nothing to cancel.
            me.cancel = None;
        }
        polled
    }
}

impl<T> Drop for RowStream<'_, T> {
    fn drop(&mut self) {
        let Some(cancel) = self.cancel.take() else {
            return;
        };
        cancel.spawn_fire();
    }
}

impl Client {
    /// Execute `sql` and stream the resulting rows lazily, page by page, without
    /// buffering the whole result set in memory.
    ///
    /// Trino returns results as a chain of pages linked by `nextUri`. This method
    /// first drives the query far enough to resolve the result schema (so
    /// [`RowStream::columns`] is available up front), then hands back a
    /// [`RowStream`] that follows the remaining pages on demand, yielding each
    /// row as it is decoded. Prefer it over [`Client::get_all`] for large result
    /// sets.
    ///
    /// Both the Direct and (with the `spooling` feature) Spooled protocols are
    /// supported. With spooling, each page's segments are fetched concurrently
    /// and decoded together, so peak memory is bounded by the largest page
    /// rather than by the whole result set.
    ///
    /// Unlike [`Client::get_all`], this does not reject a query that mixes the
    /// Direct and Spooled protocols across pages; each page is decoded according
    /// to its own protocol.
    ///
    /// The returned stream borrows `self`, so it must not outlive the [`Client`].
    ///
    /// # Example
    /// ```no_run
    /// # use trino_rust_client::{client::ClientBuilder, Row};
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use futures::StreamExt;
    ///
    /// let client = ClientBuilder::new("user", "localhost").port(8080).build()?;
    /// let mut rows = client.stream::<Row>("SELECT 1").await?;
    /// println!("columns: {:?}", rows.columns());
    /// while let Some(row) = rows.next().await {
    ///     let row = row?;
    ///     // use row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stream<'a, T>(&'a self, sql: impl Into<String>) -> Result<RowStream<'a, T>>
    where
        T: Trino + Send + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        // The pager primes the query until the schema is known and owns the
        // per-page decoding (Direct and Spooled alike), so this is a thin
        // flattening of pages into rows.
        let mut pager = QueryPager::new(self, sql.into()).await?;
        // Span stored on the RowStream and entered on each `poll_next`, so
        // events emitted while streaming carry the query_id. (Entering it here
        // across the priming `.await`s would be the guard-across-await
        // anti-pattern; priming emits little, so it is left unspanned.)
        let span = tracing::info_span!("query_stream", query_id = %pager.query_id());
        let columns = pager.columns().to_vec();
        let query_id = pager.query_id().to_string();
        // Hand cancellation to the RowStream so the pager's own Drop stays
        // inert while it lives inside the stream.
        let cancel = pager.take_cancel();

        let inner = async_stream::try_stream! {
            let mut pager = pager;
            while let Some(rows) = pager.next_page().await? {
                for row in rows {
                    yield row;
                }
            }
        };

        Ok(RowStream {
            columns,
            query_id,
            cancel,
            client: self,
            span,
            inner: Box::pin(inner),
        })
    }

    /// Run `sql` and return the whole result set as a [`DataSet`].
    ///
    /// The entire result is buffered in memory — for large results prefer
    /// [`stream`](Client::stream). `T` is a `#[derive(Trino)]` row struct, or
    /// [`Row`] for a dynamically-typed result.
    #[tracing::instrument(skip_all, fields(query_id = tracing::field::Empty))]
    pub async fn get_all<T>(&self, sql: impl Into<String>) -> Result<DataSet<T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de> + serde::Serialize,
    {
        // One paging implementation for every entry point: the pager primes the
        // query, caches the column metadata and decodes each page, so a query
        // whose pages switch between the Direct and Spooled protocols works.
        let mut pager = QueryPager::new(self, sql.into()).await?;
        tracing::Span::current().record("query_id", pager.query_id());

        let mut all_rows: Vec<T> = Vec::new();
        while let Some(rows) = pager.next_page().await? {
            all_rows.extend(rows);
        }

        let columns = match pager.columns() {
            [] => None,
            cols => Some(cols.to_vec()),
        };
        build_dataset(all_rows, columns)
    }

    #[cfg(feature = "spooling")]
    pub(crate) async fn fetch_spooled_data<T: Trino + 'static>(
        &self,
        spooled: SpooledData,
        columns: Option<Vec<crate::models::Column>>,
    ) -> Result<DataSet<T>> {
        let segment_bytes = self
            .segment_fetcher
            .fetch_segments(spooled.segments)
            .await?;

        let dataset = self.decode_segments::<T>(&spooled.encoding, segment_bytes, columns)?;

        Ok(dataset)
    }

    #[cfg(feature = "spooling")]
    fn decode_segments<T: Trino + 'static>(
        &self,
        encoding: &str,
        segment_bytes: Vec<Vec<u8>>,
        columns: Option<Vec<crate::models::Column>>,
    ) -> Result<DataSet<T>> {
        let cols = columns.ok_or_else(|| {
            Error::Protocol("Column metadata required for spooling protocol".to_string())
        })?;

        let mut all_rows: Vec<Vec<serde_json::Value>> = Vec::new();

        let encoding = SpoolingEncoding::try_from(encoding).map_err(|e| {
            Error::Decode(format!(
                "Failed to parse encoding: {}. Only 'json' based formats are supported.",
                e
            ))
        })?;

        for bytes in segment_bytes {
            let json_str = decompress_segment_bytes(&bytes, &encoding)?;

            let mut rows: Vec<Vec<serde_json::Value>> = serde_json::from_str(&json_str)
                .map_err(|e| Error::Decode(format!("Failed to parse segment JSON: {}", e)))?;

            all_rows.append(&mut rows);
        }

        let json_obj = serde_json::json!({
            "columns": cols,
            "data": all_rows
        });

        let dataset: DataSet<T> = serde_json::from_value(json_obj)
            .map_err(|e| Error::Decode(format!("Failed to deserialize DataSet: {}", e)))?;

        Ok(dataset)
    }

    /**
     * Execute a SQL statement and return the result.
     * If the TRINO query returns an error, the method returns an error of type `Error::Query`
     * @param sql The SQL statement to execute
     * @return [`Result<ExecuteResult>`]` The result of the execution
     * */
    #[tracing::instrument(skip_all, fields(query_id = tracing::field::Empty))]
    pub async fn execute(&self, sql: impl Into<String>) -> Result<ExecuteResult> {
        let mut res = self.get_retry::<Row>(sql.into()).await?;
        tracing::Span::current().record("query_id", res.id.as_str());

        // Drive the statement to completion, keeping the terminal page: it
        // already carries updateType/updateCount/error, so there is nothing to
        // re-fetch afterwards. Re-fetching used to skip auth and the retry
        // policy, which made one transient failure fatal to the statement.
        while let Some(url) = res.next_uri.clone() {
            res = self.get_next_retry::<Row>(&url).await?;
        }

        if let Some(error) = res.error {
            return Err(error.into());
        }

        Ok(ExecuteResult {
            output_uri: None,
            update_type: res.update_type,
            update_count: res.update_count,
        })
    }

    /// The session state as Trino has most recently reported it.
    ///
    /// `USE`, `SET SESSION`, `SET ROLE` and `PREPARE` all change state on the
    /// coordinator, which reports each change back on the response; the client
    /// tracks it. This is how to read the result — after `USE postgresql.public`,
    /// `snapshot.catalog` is `postgresql`, not whatever was configured at build
    /// time.
    ///
    /// The snapshot is taken under one read lock, so its fields are mutually
    /// consistent.
    pub async fn session_snapshot(&self) -> SessionSnapshot {
        self.session.read().await.snapshot()
    }

    /// The transaction this client's session is currently bound to.
    pub async fn transaction_id(&self) -> TransactionId {
        self.session.read().await.transaction_id.clone()
    }

    /// Bind the session to a transaction.
    ///
    /// Normally unnecessary — [`begin_transaction`](Self::begin_transaction)
    /// captures the identifier Trino issues. Use this to adopt a transaction
    /// started elsewhere.
    pub async fn set_transaction_id(&self, id: TransactionId) {
        self.session.write().await.transaction_id = id;
    }

    /// Start a transaction.
    ///
    /// Issues `START TRANSACTION` and captures the identifier Trino returns, so
    /// statements issued afterwards on this client run inside the transaction
    /// until [`commit`](Self::commit) or [`rollback`](Self::rollback).
    ///
    /// # Concurrency
    ///
    /// A transaction is a property of the whole client, so treat a client as
    /// single-threaded for as long as one is open. Statements already in flight
    /// when the transaction starts do not join it, and statements issued
    /// concurrently from another task will run inside it whether or not that
    /// was intended.
    ///
    /// The nesting check below is best-effort, not atomic: the session lock is
    /// released before `START TRANSACTION` is sent (holding it would deadlock
    /// against the write lock taken when the response is processed). Two tasks
    /// calling this concurrently can therefore both pass the check and open two
    /// transactions, of which only the last is retained — the other is orphaned
    /// on the coordinator until it times out. Use a separate client per
    /// transaction if you need concurrency.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transaction`] if a transaction is already active —
    /// Trino does not support nested transactions.
    ///
    /// Also returns [`Error::Transaction`] if the statement succeeded but no
    /// usable identifier came back in `X-Trino-Started-Transaction-Id`. A
    /// healthy coordinator always sends it, so this signals something between
    /// client and coordinator dropping or rewriting the header. The
    /// transaction may be open on the coordinator, and because its identifier
    /// never reached the client it cannot be committed or rolled back — it
    /// stays open until the coordinator times it out. Surfacing that as an
    /// error is what lets `Ok(())` mean a transaction is genuinely active.
    ///
    /// When the statement itself fails the transaction may nevertheless have
    /// been started, since the identifier is captured before the statement
    /// finishes. Call [`rollback`](Self::rollback) to discard it; that also
    /// clears an identifier the coordinator has already expired.
    pub async fn begin_transaction(&self) -> Result<()> {
        // Bind the guard to a local: holding it across `execute` would deadlock
        // against the write lock `update_session` takes.
        let active = self.session.read().await.transaction_id.is_active();
        if active {
            return Err(Error::Transaction(
                "a transaction is already active; Trino does not support nested transactions"
                    .to_string(),
            ));
        }
        self.execute("START TRANSACTION").await?;

        // `execute` drains every page and each response passes through
        // `update_session`, so the identifier has had every chance to arrive by
        // now. If it still has not, the session is not in a transaction and
        // reporting success would put every later statement back on `NONE` —
        // the silent failure this API exists to prevent.
        if !self.session.read().await.transaction_id.is_active() {
            return Err(Error::Transaction(
                "START TRANSACTION succeeded but no usable transaction id was returned in \
                 X-Trino-Started-Transaction-Id; the session is not in a transaction"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Commit the active transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transaction`] if no transaction is active.
    pub async fn commit(&self) -> Result<()> {
        self.end_transaction("COMMIT").await
    }

    /// Roll back the active transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transaction`] if no transaction is active.
    pub async fn rollback(&self) -> Result<()> {
        self.end_transaction("ROLLBACK").await
    }

    /// Shared implementation of [`commit`](Self::commit) and
    /// [`rollback`](Self::rollback).
    ///
    /// Trino answers either with `X-Trino-Clear-Transaction-Id`, which
    /// `update_session` turns back into `TransactionId::NoTransaction`.
    ///
    /// Deliberately without the post-condition [`begin_transaction`]
    /// (Self::begin_transaction) carries. If the clear header never arrives the
    /// session keeps an identifier the coordinator has already retired, but
    /// that fails loudly: the next statement sends the dead id and Trino
    /// rejects it. The `begin_transaction` case is the dangerous one because
    /// there the session falls back to `NONE`, which Trino happily accepts as
    /// "no transaction" and executes outside any transaction. Callers that
    /// need to recover here can reset the session with
    /// [`set_transaction_id`](Self::set_transaction_id).
    async fn end_transaction(&self, statement: &str) -> Result<()> {
        let active = self.session.read().await.transaction_id.is_active();
        if !active {
            return Err(Error::Transaction(format!(
                "no active transaction to {}",
                statement.to_lowercase()
            )));
        }
        self.execute(statement).await?;
        Ok(())
    }

    fn retry_policy(&self) -> ExponentialBuilder {
        self.retry.backoff()
    }

    pub(crate) async fn get_retry<T>(&self, sql: String) -> Result<QueryResult<T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        let result = || async { self.get::<T>(sql.clone()).await };

        // Submission is not idempotent — retry only when definitely not processed.
        result
            .retry(self.retry_policy())
            .when(need_retry_submit)
            .await
    }

    pub(crate) async fn get_next_retry<T>(&self, url: &str) -> Result<QueryResult<T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        let result = || async { self.get_next(url).await };

        // Page fetches are idempotent GETs — any transient failure is retryable.
        result
            .retry(self.retry_policy())
            .when(need_retry_fetch)
            .await
    }

    /// Everything needed to fire a best-effort cancellation for `query_id`
    /// without borrowing `self`.
    pub(crate) fn cancel_on_drop(&self, query_id: &str) -> CancelOnDrop {
        CancelOnDrop {
            client: self.client.clone(),
            url: format!("{}v1/query/{}", self.url, query_id),
            auth: self.auth.clone(),
        }
    }

    /// Submit `sql` and return a [`QueryPager`] that yields one page of rows at
    /// a time.
    ///
    /// Use this when you need rows incrementally *and* need the query to stay
    /// cancellable — the pager exposes [`QueryPager::query_id`] and
    /// [`QueryPager::cancel`]. For simple cases prefer [`get_all`](Self::get_all)
    /// (buffers everything) or [`stream`](Self::stream) (row-at-a-time).
    ///
    /// Unlike hand-rolled paging over [`get`](Self::get) / [`get_next`](Self::get_next),
    /// this resolves spooled result data correctly.
    #[tracing::instrument(skip_all, fields(query_id = tracing::field::Empty))]
    pub async fn pager<T>(&self, sql: impl Into<String>) -> Result<QueryPager<'_, T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        let pager = QueryPager::new(self, sql.into()).await?;
        tracing::Span::current().record("query_id", pager.query_id());
        Ok(pager)
    }

    /// Submit `sql` and return the first result page.
    ///
    /// Low-level building block: the returned [`QueryResult`] may carry a
    /// `next_uri` that you must follow with [`get_next`](Client::get_next) to
    /// retrieve the rest. Most callers should use [`get_all`](Client::get_all)
    /// or [`stream`](Client::stream), which handle pagination.
    pub async fn get<T>(&self, sql: impl Into<String>) -> Result<QueryResult<T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        let req = self
            .client
            .post(format!("{}v1/statement", self.url))
            .body(sql.into());
        let req = {
            let session = self.session.read().await;
            add_session_header(req, &session)
        };

        self.send(req, StatusCode::OK, |resp| async {
            let text = resp.text().await?;

            let data: QueryResult<T> = serde_json::from_str(&text)
                .map_err(|e| Error::Decode(format!("Failed to parse response: {}", e)))?;
            Ok(data)
        })
        .await
    }

    /// Fetch the next result page from a `next_uri` returned by a previous
    /// [`get`](Client::get) / `get_next` call.
    pub async fn get_next<T>(&self, url: &str) -> Result<QueryResult<T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        let req = self.client.get(url);
        let req = {
            let session = self.session.read().await;
            add_prepare_header(req, &session)
        };

        self.send(req, StatusCode::OK, |resp| async {
            let text = resp.text().await?;
            let data: QueryResult<T> = serde_json::from_str(&text)
                .map_err(|e| Error::Decode(format!("Failed to parse response: {}", e)))?;
            Ok(data)
        })
        .await
    }

    /// Cancel a running query by its id, releasing its resources on the
    /// coordinator.
    pub async fn cancel(&self, query_id: &str) -> Result<()> {
        let url = format!("{}v1/query/{}", self.url, query_id);
        let req = self.client.delete(url);
        let req = {
            let session = self.session.read().await;
            add_prepare_header(req, &session)
        };

        self.send(req, StatusCode::NO_CONTENT, |_| async { Ok(()) })
            .await
    }

    fn auth_req(&self, req: RequestBuilder) -> RequestBuilder {
        if let Some(auth) = self.auth.as_ref() {
            match auth {
                Auth::Basic(u, p) => req.basic_auth(u, p.as_ref()),
                Auth::Jwt(t) => req.bearer_auth(t),
                // Tokens are acquired lazily: with nothing cached the request
                // goes out unauthenticated, and `send` runs the login flow on
                // the resulting 401 challenge before retrying once.
                Auth::OAuth2(state) => match state.cached_token() {
                    Some(t) => req.bearer_auth(t),
                    None => req,
                },
            }
        } else {
            req
        }
    }

    async fn send<R, F, Fut>(
        &self,
        req: RequestBuilder,
        expected_status: StatusCode,
        handle_response: F,
    ) -> Result<R>
    where
        F: FnOnce(Response) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        // Capture the token we are about to authenticate with (if any) so the
        // single-flight refresh can tell whether another task already rotated it.
        let sent_token = match self.auth.as_ref() {
            Some(Auth::OAuth2(state)) => state.cached_token(),
            _ => None,
        };
        // Clone the UN-authed builder up front so an OAuth2 401 can be retried
        // with exactly one fresh token header (`bearer_auth` appends, so auth is
        // applied only after the clone is taken). Bodies here are `String`s, so
        // `try_clone` always succeeds.
        let retry_req = req.try_clone();
        let resp = self.auth_req(req).send().await?;

        if resp.status() == StatusCode::UNAUTHORIZED {
            if let (Some(Auth::OAuth2(state)), Some(retry_req)) = (self.auth.as_ref(), retry_req) {
                // A coordinator with several authentication types configured
                // (e.g. `http-server.authentication.type=PASSWORD,OAUTH2`) sends
                // one `WWW-Authenticate` header per type, in configuration
                // order — so `Basic realm="Trino"` may well precede the Bearer
                // challenge. Scan all of them for the OAuth2 one.
                if let Some(challenge) = resp
                    .headers()
                    .get_all(reqwest::header::WWW_AUTHENTICATE)
                    .iter()
                    .filter_map(|v| v.to_str().ok())
                    .find_map(crate::auth::parse_www_authenticate)
                {
                    self.acquire_oauth2_token(state, &challenge, sent_token)
                        .await?;
                    let resp = self.auth_req(retry_req).send().await?;
                    return self
                        .finish_send(resp, expected_status, handle_response)
                        .await;
                }
            }
        }

        self.finish_send(resp, expected_status, handle_response)
            .await
    }

    /// Shared response-status handling (extracted so both the first and the
    /// retried OAuth2 request go through the same path).
    async fn finish_send<R, F, Fut>(
        &self,
        resp: Response,
        expected_status: StatusCode,
        handle_response: F,
    ) -> Result<R>
    where
        F: FnOnce(Response) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        let status = resp.status();
        if status != expected_status {
            let data = resp.text().await.unwrap_or("".to_string());
            Err(Error::HttpNotOk(status, data))
        } else {
            self.update_session(&resp).await;
            handle_response(resp).await
        }
    }

    /// Acquire an OAuth2 token under a single-flight lock: if another task
    /// already refreshed while we waited, reuse that token instead of opening a
    /// second browser.
    async fn acquire_oauth2_token(
        &self,
        state: &std::sync::Arc<crate::auth::OAuth2State>,
        challenge: &crate::auth::Challenge,
        sent_token: Option<String>,
    ) -> Result<()> {
        let _guard = state.acquire.lock().await;
        // Someone else finished the flow (rotating the token this request was
        // sent with) while we waited for the lock — reuse it.
        if state.cached_token() != sent_token {
            return Ok(());
        }
        let token = crate::auth::run_flow(&self.client, state, challenge).await?;
        *state.token.write().unwrap() = Some(token);
        Ok(())
    }

    async fn update_session(&self, resp: &Response) {
        let mut session = self.session.write().await;

        set_header!(session.catalog, HEADER_SET_CATALOG, resp);
        set_header!(session.schema, HEADER_SET_SCHEMA, resp);
        set_header!(session.path, HEADER_SET_PATH, resp);

        set_header_map!(session.properties, HEADER_SET_SESSION, resp);
        clear_header_map!(session.properties, HEADER_CLEAR_SESSION, resp);

        set_header_map!(session.roles, HEADER_SET_ROLE, resp, SelectedRole::from_str);

        set_header_map!(session.prepared_statements, HEADER_ADDED_PREPARE, resp);
        clear_header_map!(
            session.prepared_statements,
            HEADER_DEALLOCATED_PREPARE,
            resp
        );

        if let Some(v) = resp.headers().get(HEADER_STARTED_TRANSACTION_ID) {
            match v.to_str() {
                Ok(s) => session.transaction_id = TransactionId::from_header_value(s),
                Err(e) => warn!(
                    "parse header {} failed, reason: {}",
                    HEADER_STARTED_TRANSACTION_ID, e
                ),
            }
        }
        clear_header!(session.transaction_id, HEADER_CLEAR_TRANSACTION_ID, resp);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////
// helper functions

fn encode_kv(k: &str, v: &str) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .append_pair(k, v)
        .finish()
}

fn decode_kv_from_header(input: &HeaderValue) -> Option<(String, String)> {
    let kvs = url::form_urlencoded::parse(input.as_bytes()).collect::<Vec<_>>();
    if kvs.is_empty() {
        None
    } else {
        Some((kvs[0].0.to_string(), kvs[0].1.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use reqwest::header::HeaderValue;

    use super::*;
    use crate::client::{decode_kv_from_header, need_retry_fetch, need_retry_submit};
    use crate::error::Error;
    use crate::session::SessionBuilder;
    use crate::transaction::TransactionId;

    #[test]
    fn builtin_capabilities_are_always_present_and_sorted() {
        let session = SessionBuilder::new("u", "h").build().unwrap();
        assert_eq!(
            client_capabilities_value(&session),
            "PARAMETRIC_DATETIME,PATH"
        );
    }

    #[test]
    fn extra_capabilities_merge_with_the_builtins() {
        let mut b = SessionBuilder::new("u", "h");
        // A duplicate of a built-in must not be emitted twice.
        b.client_capabilities.insert("PATH".to_string());
        b.client_capabilities
            .insert("SOME_FUTURE_THING".to_string());
        let session = b.build().unwrap();
        assert_eq!(
            client_capabilities_value(&session),
            "PARAMETRIC_DATETIME,PATH,SOME_FUTURE_THING"
        );
    }

    #[test]
    fn test_decode_kv_from_header_plus_sign_to_space() {
        let header_value = HeaderValue::from_static("statement=show+tables");
        let result = decode_kv_from_header(&header_value);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "statement");
        assert_eq!(value, "show tables");
    }

    #[test]
    fn test_decode_kv_from_header_percent_encoding() {
        let header_value = HeaderValue::from_static("statement=show%20tables");
        let result = decode_kv_from_header(&header_value);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "statement");
        assert_eq!(value, "show tables");
    }

    fn http_not_ok(code: StatusCode) -> Error {
        Error::HttpNotOk(code, String::new())
    }

    #[test]
    fn fetch_retries_all_transient_statuses() {
        // Idempotent page fetches retry every transient gateway/availability status.
        for code in [
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            assert!(need_retry_fetch(&http_not_ok(code)), "{code}");
        }
        // Client errors and non-transient 5xx fail fast.
        for code in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(!need_retry_fetch(&http_not_ok(code)), "{code}");
        }
        assert!(!need_retry_fetch(&Error::Protocol(
            "mixed protocols".into()
        )));
        assert!(!need_retry_fetch(&Error::InconsistentData));
    }

    #[test]
    fn submit_only_retries_definitely_unprocessed() {
        // Submission is non-idempotent: only 503 (rejected, not processed) is retried.
        assert!(need_retry_submit(&http_not_ok(
            StatusCode::SERVICE_UNAVAILABLE
        )));
        // 502/504 are ambiguous (a proxy may have forwarded the query) -> terminal.
        assert!(!need_retry_submit(&http_not_ok(StatusCode::BAD_GATEWAY)));
        assert!(!need_retry_submit(&http_not_ok(
            StatusCode::GATEWAY_TIMEOUT
        )));
        assert!(!need_retry_submit(&http_not_ok(
            StatusCode::INTERNAL_SERVER_ERROR
        )));
    }

    #[tokio::test]
    async fn transaction_id_defaults_to_no_transaction() {
        let client = ClientBuilder::new("user", "localhost").build().unwrap();
        assert_eq!(client.transaction_id().await, TransactionId::NoTransaction);
    }

    #[tokio::test]
    async fn set_transaction_id_is_observable() {
        let client = ClientBuilder::new("user", "localhost").build().unwrap();
        let id = TransactionId::Id("17cbc429-462a-4da3-9a06-02b6507d0d01".to_string());
        client.set_transaction_id(id.clone()).await;
        assert_eq!(client.transaction_id().await, id);
    }

    #[tokio::test]
    async fn begin_transaction_rejects_nesting() {
        let client = ClientBuilder::new("user", "localhost").build().unwrap();
        client
            .set_transaction_id(TransactionId::Id("abc".to_string()))
            .await;

        let err = client.begin_transaction().await.unwrap_err();
        assert!(
            matches!(err, Error::Transaction(_)),
            "expected Error::Transaction, got {err:?}"
        );
    }

    #[tokio::test]
    async fn commit_without_transaction_is_rejected() {
        let client = ClientBuilder::new("user", "localhost").build().unwrap();
        let err = client.commit().await.unwrap_err();
        assert!(
            matches!(err, Error::Transaction(_)),
            "expected Error::Transaction, got {err:?}"
        );
    }

    #[tokio::test]
    async fn rollback_without_transaction_is_rejected() {
        let client = ClientBuilder::new("user", "localhost").build().unwrap();
        let err = client.rollback().await.unwrap_err();
        assert!(
            matches!(err, Error::Transaction(_)),
            "expected Error::Transaction, got {err:?}"
        );
    }
}
