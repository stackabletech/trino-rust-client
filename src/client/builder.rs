use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono_tz::Tz;
use tokio::sync::RwLock;

use crate::auth::Auth;
use crate::client::Client;
use crate::error::{Error, Result};
use crate::retry::RetryPolicy;
use crate::selected_role::SelectedRole;
use crate::session::SessionBuilder;
#[cfg(feature = "spooling")]
use crate::spooling::{SegmentFetcher, SpoolingEncoding};
use crate::ssl::Ssl;
use crate::transaction::TransactionId;

/// Builder for a [`Client`].
///
/// Start with [`ClientBuilder::new`], chain the setters you need, then call
/// [`build`](ClientBuilder::build).
///
/// ```no_run
/// # use trino_rust_client::client::ClientBuilder;
/// # fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let client = ClientBuilder::new("user", "trino.example.com")
///     .port(8443)
///     .secure(true)
///     .catalog("hive")
///     .schema("default")
///     .build()?;
/// # Ok(()) }
/// ```
pub struct ClientBuilder {
    session: SessionBuilder,
    auth: Option<Auth>,
    auth_http_insecure: bool,
    retry: RetryPolicy,
    ssl: Option<Ssl>,
    no_verify: bool,
    #[cfg(feature = "spooling")]
    segment_fetcher: Option<SegmentFetcher>,
    #[cfg(feature = "spooling")]
    max_concurrent_segments: Option<usize>,
}

impl ClientBuilder {
    /// Start building a client for the given Trino `user` and `host`.
    ///
    /// Defaults: port 8080, plain HTTP, no authentication. Use the setters to
    /// change them, then call [`build`](ClientBuilder::build).
    pub fn new(user: impl ToString, host: impl ToString) -> Self {
        let builder = SessionBuilder::new(user, host);
        Self {
            session: builder,
            auth: None,
            auth_http_insecure: false,
            retry: RetryPolicy::default(),
            ssl: None,
            no_verify: false,
            #[cfg(feature = "spooling")]
            segment_fetcher: None,
            #[cfg(feature = "spooling")]
            max_concurrent_segments: None,
        }
    }

    pub fn port(mut self, s: u16) -> Self {
        self.session.port = s;
        self
    }

    pub fn secure(mut self, s: bool) -> Self {
        self.session.secure = s;
        self
    }

    pub fn no_verify(mut self, nv: bool) -> Self {
        self.no_verify = nv;
        self
    }

    pub fn source(mut self, s: impl ToString) -> Self {
        self.session.source = s.to_string();
        self
    }

    pub fn trace_token(mut self, s: impl ToString) -> Self {
        self.session.trace_token = Some(s.to_string());
        self
    }

    pub fn client_tags(mut self, s: HashSet<String>) -> Self {
        self.session.client_tags = s;
        self
    }

    pub fn client_tag(mut self, s: impl ToString) -> Self {
        self.session.client_tags.insert(s.to_string());
        self
    }

    pub fn client_info(mut self, s: impl ToString) -> Self {
        self.session.client_info = Some(s.to_string());
        self
    }

    pub fn catalog(mut self, s: impl ToString) -> Self {
        self.session.catalog = Some(s.to_string());
        self
    }

    pub fn schema(mut self, s: impl ToString) -> Self {
        self.session.schema = Some(s.to_string());
        self
    }

    pub fn path(mut self, s: impl ToString) -> Self {
        self.session.path = Some(s.to_string());
        self
    }

    /// Set the session time zone, sent as `X-Trino-Time-Zone`.
    ///
    /// Without this the coordinator's own default applies, which determines how
    /// `timestamp with time zone` values are rendered.
    ///
    /// ```no_run
    /// # use trino_rust_client::{client::ClientBuilder, Tz};
    /// # fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = ClientBuilder::new("user", "localhost")
    ///     .timezone("Europe/Berlin".parse::<Tz>()?)
    ///     .build()?;
    /// # Ok(()) }
    /// ```
    pub fn timezone(mut self, tz: Tz) -> Self {
        self.session.timezone = Some(tz);
        self
    }

    /// Replace the whole per-catalog role map, sent as `X-Trino-Role`.
    pub fn roles(mut self, s: HashMap<String, SelectedRole>) -> Self {
        self.session.roles = s;
        self
    }

    /// Select a role for one catalog, sent as `X-Trino-Role`.
    ///
    /// `catalog` is a catalog name, or the special key `"system"` for the system
    /// role. Hive and Iceberg under `sql-standard` security need this; it is also
    /// the only configuration in which `SQLTablePrivileges` returns a row.
    ///
    /// ```no_run
    /// # use trino_rust_client::client::ClientBuilder;
    /// # use trino_rust_client::selected_role::{RoleType, SelectedRole};
    /// # fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = ClientBuilder::new("user", "localhost")
    ///     .role("hive", SelectedRole::new(RoleType::Role, Some("admin".into())))
    ///     .build()?;
    /// # Ok(()) }
    /// ```
    pub fn role(mut self, catalog: impl ToString, role: SelectedRole) -> Self {
        self.session.roles.insert(catalog.to_string(), role);
        self
    }

    /// Run statements as `u` while authenticating as the user given to
    /// [`ClientBuilder::new`] — impersonation, sent as `X-Trino-User`.
    ///
    /// The coordinator only allows this if the authenticating principal holds
    /// the `impersonate` permission for the target user.
    ///
    /// Builder-time only, deliberately: session state (properties, roles,
    /// prepared statements) accumulates on a client, so one client per
    /// impersonated user keeps that state from leaking across identities.
    pub fn session_user(mut self, u: impl ToString) -> Self {
        self.session.session_user = Some(u.to_string());
        self
    }

    /// Set the locale sent as `X-Trino-Language`, e.g. `"de-DE"`.
    pub fn locale(mut self, l: impl ToString) -> Self {
        self.session.locale = Some(l.to_string());
        self
    }

    pub fn resource_estimates(mut self, s: HashMap<String, String>) -> Self {
        self.session.resource_estimates = s;
        self
    }

    pub fn resource_estimate(mut self, k: impl ToString, v: impl ToString) -> Self {
        self.session
            .resource_estimates
            .insert(k.to_string(), v.to_string());
        self
    }

    pub fn properties(mut self, s: HashMap<String, String>) -> Self {
        self.session.properties = s;
        self
    }

    pub fn property(mut self, k: impl ToString, v: impl ToString) -> Self {
        self.session.properties.insert(k.to_string(), v.to_string());
        self
    }

    pub fn prepared_statements(mut self, s: HashMap<String, String>) -> Self {
        self.session.prepared_statements = s;
        self
    }

    pub fn prepared_statement(mut self, k: impl ToString, v: impl ToString) -> Self {
        self.session
            .prepared_statements
            .insert(k.to_string(), v.to_string());
        self
    }

    pub fn extra_credentials(mut self, s: HashMap<String, String>) -> Self {
        self.session.extra_credentials = s;
        self
    }

    pub fn extra_credential(mut self, k: impl ToString, v: impl ToString) -> Self {
        self.session
            .extra_credentials
            .insert(k.to_string(), v.to_string());
        self
    }

    pub fn transaction_id(mut self, s: TransactionId) -> Self {
        self.session.transaction_id = s;
        self
    }

    pub fn client_request_timeout(mut self, s: Duration) -> Self {
        self.session.client_request_timeout = s;
        self
    }

    pub fn compression_disabled(mut self, s: bool) -> Self {
        self.session.compression_disabled = s;
        self
    }

    #[cfg(feature = "spooling")]
    pub fn segment_fetcher(mut self, segment_fetcher: SegmentFetcher) -> Self {
        self.segment_fetcher = Some(segment_fetcher);
        self
    }

    #[cfg(feature = "spooling")]
    /// Set the maximum number of concurrent segment fetches
    /// Default is based on available CPU parallelism (minimum 1)
    pub fn max_concurrent_segments(mut self, count: usize) -> Self {
        self.max_concurrent_segments = Some(count);
        self
    }

    #[cfg(feature = "spooling")]
    /// Advertise Trino's spooling protocol to the coordinator using `encoding`.
    ///
    /// Spooling is **opt-in**: without this call the client does not send
    /// `X-Trino-Query-Data-Encoding`, and the coordinator returns rows inline.
    ///
    /// To pick an encoding at runtime, convert first:
    /// `SpoolingEncoding::try_from(s.as_str())?`.
    pub fn spooling_encoding(mut self, encoding: SpoolingEncoding) -> Self {
        self.session.spooling_encoding = Some(encoding);
        self
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    pub fn auth(mut self, s: Auth) -> Self {
        self.auth = Some(s);
        self
    }

    pub fn auth_http_insecure(mut self, ahi: bool) -> Self {
        self.auth_http_insecure = ahi;
        self
    }

    pub fn max_attempt(mut self, s: usize) -> Self {
        self.retry.max_retries = s;
        self
    }

    /// Set the full retry/backoff policy for transient failures.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    pub fn ssl(mut self, ssl: Ssl) -> Self {
        self.ssl = Some(ssl);
        self
    }

    pub fn build(self) -> Result<Client> {
        let session = self.session.build()?;
        let retry = self.retry.clone();

        if (self.auth.is_some() && session.url.scheme() == "http") && !self.auth_http_insecure {
            return Err(Error::BasicAuthWithHttp);
        }

        let mut client_builder =
            reqwest::ClientBuilder::new().timeout(session.client_request_timeout);

        if self.no_verify {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }

        if let Some(ssl) = &self.ssl {
            if let Some(root) = &ssl.root_cert {
                client_builder = client_builder.add_root_certificate(root.0.clone());
            }
        }

        let client = client_builder.build()?;

        #[cfg(feature = "spooling")]
        let segment_fetcher = {
            let mut fetcher = self.segment_fetcher.unwrap_or_else(|| {
                let mut f = SegmentFetcher::new(client.clone());
                if let Some(max_concurrent) = self.max_concurrent_segments {
                    f = f.with_max_concurrent(max_concurrent);
                }
                f
            });
            // Applies to a caller-supplied fetcher too: one policy per client.
            fetcher.set_retry_policy(retry.clone());
            fetcher
        };

        let cli = Client {
            auth: self.auth,
            url: session.url.clone(),
            session: RwLock::new(session),
            client,
            retry,
            #[cfg(feature = "spooling")]
            segment_fetcher,
        };

        Ok(cli)
    }
}
