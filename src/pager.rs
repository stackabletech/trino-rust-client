//! Manual, page-at-a-time iteration over a query's results.

use crate::client::{CancelOnDrop, Client};
#[cfg(not(feature = "spooling"))]
use crate::error::Error;
use crate::error::Result;
use crate::models::{Column, QueryResult, QueryResultData};
use crate::Trino;

/// A handle to one running query, yielding its results one page at a time.
///
/// Created by [`Client::pager`]. Unlike [`Client::get_all`] it does not buffer
/// the whole result set, and unlike [`Client::stream`] it hands back whole pages
/// and exposes [`query_id`](Self::query_id) and an explicit
/// [`cancel`](Self::cancel) — the combination a driver needs to keep a query
/// cancellable while delivering rows incrementally.
///
/// Spooled pages are resolved transparently: segments are fetched (concurrently,
/// honouring the client's [`RetryPolicy`](crate::retry::RetryPolicy)) and decoded
/// against the query's column metadata, so callers see the same rows either way.
///
/// # Cancellation
/// Dropping a pager before it is exhausted best-effort cancels the query on the
/// coordinator (fire-and-forget `DELETE`, requires an active Tokio runtime).
/// [`cancel`](Self::cancel) does the same thing but awaits it and reports
/// failure. Cancellation is skipped once the query has finished normally.
pub struct QueryPager<'a, T: Trino> {
    client: &'a Client,
    query_id: String,
    columns: Vec<Column>,
    /// Cursor for the next page; `None` once the coordinator stops sending one.
    next_uri: Option<String>,
    /// The page consumed while priming to resolve `columns`, not yet returned.
    pending: Option<QueryResult<T>>,
    cancel: Option<CancelOnDrop>,
}

impl<'a, T> QueryPager<'a, T>
where
    T: Trino + 'static,
    for<'de> T: serde::Deserialize<'de>,
{
    /// Submit `sql` and drive the query until its schema is known.
    pub(crate) async fn new(client: &'a Client, sql: String) -> Result<Self> {
        // Follow pages until one carries `columns` (or data, or the query ends),
        // so `columns()` and `query_id()` are usable before the first row.
        let mut res = client.get_retry::<T>(sql).await?;
        loop {
            if let Some(error) = res.error.take() {
                return Err(error.into());
            }
            if res.columns.is_some() || res.data.is_some() {
                break;
            }
            match res.next_uri.clone() {
                Some(url) => res = client.get_next_retry::<T>(&url).await?,
                None => break,
            }
        }

        let query_id = res.id.clone();
        let columns = res.columns.clone().unwrap_or_default();
        let cancel = Some(client.cancel_on_drop(&query_id));

        Ok(Self {
            client,
            query_id,
            columns,
            next_uri: None,
            pending: Some(res),
            cancel,
        })
    }

    /// The coordinator's query id, usable with [`Client::cancel`].
    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    /// The result columns, resolved before the first page is returned.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// The next page of rows, or `None` once the query is exhausted.
    ///
    /// Trino emits pages carrying no rows while a query is still running; those
    /// are followed internally rather than surfaced, so every returned page is
    /// non-empty. Once this returns `None` it keeps returning `None`.
    pub async fn next_page(&mut self) -> Result<Option<Vec<T>>> {
        loop {
            let mut res = match self.pending.take() {
                Some(res) => res,
                None => match self.next_uri.take() {
                    Some(url) => self.client.get_next_retry::<T>(&url).await?,
                    None => {
                        // Finished normally — nothing to cancel.
                        self.cancel = None;
                        return Ok(None);
                    }
                },
            };

            if let Some(error) = res.error.take() {
                return Err(error.into());
            }

            self.next_uri = res.next_uri.take();

            // Columns can arrive on a later page than the first; keep the first
            // set we see, since spooled pages need them to decode.
            if self.columns.is_empty() {
                if let Some(cols) = res.columns.clone() {
                    self.columns = cols;
                }
            }

            let Some(data) = res.data.take() else {
                continue;
            };

            let rows = self.decode(data).await?;
            if rows.is_empty() {
                continue;
            }
            return Ok(Some(rows));
        }
    }

    async fn decode(&self, data: QueryResultData<T>) -> Result<Vec<T>> {
        match data {
            QueryResultData::Direct(rows) => Ok(rows),
            #[cfg(feature = "spooling")]
            QueryResultData::Spooled(spooled) => {
                // Decoding is column-directed, so hand over the cached metadata.
                let columns = if self.columns.is_empty() {
                    None
                } else {
                    Some(self.columns.clone())
                };
                let dataset = self.client.fetch_spooled_data::<T>(spooled, columns).await?;
                Ok(dataset.into_vec())
            }
            #[cfg(not(feature = "spooling"))]
            QueryResultData::Spooled(_) => Err(Error::Protocol(
                "server sent spooled data but the 'spooling' feature is not enabled. \
                 Add features = [\"spooling\"] to your trino-rust-client dependency \
                 in Cargo.toml."
                    .to_string(),
            )),
        }
    }

    /// Cancel the query on the coordinator and report whether it succeeded.
    pub async fn cancel(mut self) -> Result<()> {
        // Disarm the Drop impl so it does not fire a second, redundant DELETE.
        self.cancel = None;
        self.client.cancel(&self.query_id).await
    }
}

impl<T: Trino> Drop for QueryPager<'_, T> {
    fn drop(&mut self) {
        let Some(cancel) = self.cancel.take() else {
            return;
        };
        cancel.spawn_fire();
    }
}
