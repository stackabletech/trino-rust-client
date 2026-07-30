use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono_tz::Tz;
use http::uri::Scheme;
use reqwest::Url;

use crate::error::*;
use crate::selected_role::SelectedRole;
#[cfg(feature = "spooling")]
use crate::spooling::SpoolingEncoding;
use crate::transaction::TransactionId;

// TODO remove this when `std::default::default` is stabilized
fn default<T: Default>() -> T {
    Default::default()
}

#[derive(Debug)]
pub struct Session {
    pub url: Url,
    /// The Trino user, sent as `X-Trino-User`.
    ///
    /// `None` leaves the header off, which is only meaningful when the request
    /// authenticates: Trino then takes the user from the authenticated
    /// identity. Interactive OAuth 2.0 is the case that needs it — the identity
    /// provider decides who you are, and a header naming somebody else is read
    /// as an impersonation attempt rather than ignored.
    pub user: Option<String>,
    /// When set, the user statements run as (`X-Trino-User`), leaving [`Self::user`]
    /// as the authenticating principal.
    pub session_user: Option<String>,
    pub source: String,
    pub trace_token: Option<String>,
    pub client_tags: HashSet<String>,
    /// Extra capabilities advertised on top of the ones the client always sends.
    pub client_capabilities: HashSet<String>,
    pub client_info: Option<String>,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub path: Option<String>,
    // TODO: set system timezone as default value
    pub timezone: Option<Tz>,
    /// Sent as `X-Trino-Language`.
    pub locale: Option<String>,
    pub resource_estimates: HashMap<String, String>,
    pub properties: HashMap<String, String>,
    pub prepared_statements: HashMap<String, String>,
    pub roles: HashMap<String, SelectedRole>,
    pub extra_credentials: HashMap<String, String>,
    /// Extra headers sent on every request. Validated at build time not to
    /// collide with any header the client manages.
    pub extra_headers: HashMap<String, String>,
    pub transaction_id: TransactionId,
    pub client_request_timeout: Duration,
    pub compression_disabled: bool,
    #[cfg(feature = "spooling")]
    pub spooling_encoding: Option<SpoolingEncoding>,
}

#[derive(Debug)]
pub(crate) struct SessionBuilder {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) secure: bool,
    pub(crate) user: Option<String>,
    pub(crate) session_user: Option<String>,
    pub(crate) source: String,
    pub(crate) trace_token: Option<String>,
    pub(crate) client_tags: HashSet<String>,
    pub(crate) client_capabilities: HashSet<String>,
    pub(crate) client_info: Option<String>,
    pub(crate) catalog: Option<String>,
    pub(crate) schema: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) timezone: Option<Tz>,
    pub(crate) locale: Option<String>,
    pub(crate) resource_estimates: HashMap<String, String>,
    pub(crate) properties: HashMap<String, String>,
    pub(crate) prepared_statements: HashMap<String, String>,
    pub(crate) roles: HashMap<String, SelectedRole>,
    pub(crate) extra_credentials: HashMap<String, String>,
    pub(crate) extra_headers: HashMap<String, String>,
    pub(crate) transaction_id: TransactionId,
    pub(crate) client_request_timeout: Duration,
    pub(crate) compression_disabled: bool,
    #[cfg(feature = "spooling")]
    pub(crate) spooling_encoding: Option<SpoolingEncoding>,
}

impl SessionBuilder {
    pub fn new(user: impl ToString, host: impl ToString) -> SessionBuilder {
        Self::for_user(Some(user.to_string()), host)
    }

    /// A session that sends no `X-Trino-User`, leaving the user to be taken
    /// from the authenticated identity.
    ///
    /// Only meaningful with authentication configured; [`ClientBuilder::build`]
    /// rejects the combination of no user and no authentication, because Trino
    /// would answer `User must be set`.
    ///
    /// [`ClientBuilder::build`]: crate::client::ClientBuilder::build
    pub fn without_user(host: impl ToString) -> SessionBuilder {
        Self::for_user(None, host)
    }

    fn for_user(user: Option<String>, host: impl ToString) -> SessionBuilder {
        SessionBuilder {
            host: host.to_string(),
            port: 80,
            secure: false,
            user,
            session_user: None,
            source: "trino-rust-client".to_string(),
            trace_token: None,
            client_tags: default(),
            client_capabilities: default(),
            client_info: None,
            catalog: None,
            schema: None,
            path: None,
            timezone: None,
            locale: None,
            resource_estimates: default(),
            properties: default(),
            prepared_statements: default(),
            roles: default(),
            extra_credentials: default(),
            extra_headers: default(),
            transaction_id: TransactionId::NoTransaction,
            client_request_timeout: Duration::from_secs(30),
            compression_disabled: false,
            // Spooling is opt-in: advertising it is what makes the coordinator
            // spool, so it must take an explicit `spooling_encoding` call.
            #[cfg(feature = "spooling")]
            spooling_encoding: None,
        }
    }

    pub fn build(self) -> Result<Session> {
        let scheme = if self.secure {
            Scheme::HTTPS
        } else {
            Scheme::HTTP
        };
        let host = self.host;
        let s = format!("{}://{}:{}", scheme, host, self.port);
        let url = Url::parse(&s).map_err(|_| Error::InvalidHost(host))?;
        let ret = Session {
            url,
            user: self.user,
            session_user: self.session_user,
            source: self.source,
            trace_token: self.trace_token,
            client_tags: self.client_tags,
            client_capabilities: self.client_capabilities,
            client_info: self.client_info,
            catalog: self.catalog,
            schema: self.schema,
            path: self.path,
            timezone: self.timezone,
            locale: self.locale,
            resource_estimates: self.resource_estimates,
            properties: self.properties,
            prepared_statements: self.prepared_statements,
            roles: self.roles,
            extra_credentials: self.extra_credentials,
            extra_headers: self.extra_headers,
            transaction_id: self.transaction_id,
            client_request_timeout: self.client_request_timeout,
            compression_disabled: self.compression_disabled,
            #[cfg(feature = "spooling")]
            spooling_encoding: self.spooling_encoding,
        };
        Ok(ret)
    }
}

/// An owned, point-in-time copy of the session state Trino can change while a
/// client is in use.
///
/// Taken under a single read lock, so the fields are mutually consistent:
/// `catalog` and `schema` can never come from either side of a concurrent
/// `USE`. Static configuration — user, source, time zone, client tags — is not
/// included, because the caller supplied it and it never changes.
///
/// Obtained from
/// [`Client::session_snapshot`](crate::client::Client::session_snapshot).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SessionSnapshot {
    /// The current catalog, after any `X-Trino-Set-Catalog`.
    pub catalog: Option<String>,
    /// The current schema, after any `X-Trino-Set-Schema`.
    pub schema: Option<String>,
    /// The current SQL path, after any `X-Trino-Set-Path`.
    pub path: Option<String>,
    /// Session properties, after `SET`/`RESET SESSION`.
    pub properties: HashMap<String, String>,
    /// Selected roles per catalog, after `SET ROLE`.
    pub roles: HashMap<String, SelectedRole>,
    /// Statements added by `PREPARE` and not yet deallocated.
    pub prepared_statements: HashMap<String, String>,
    /// The transaction the session is bound to.
    pub transaction_id: TransactionId,
}

impl Session {
    pub(crate) fn snapshot(&self) -> SessionSnapshot {
        SessionSnapshot {
            catalog: self.catalog.clone(),
            schema: self.schema.clone(),
            path: self.path.clone(),
            properties: self.properties.clone(),
            roles: self.roles.clone(),
            prepared_statements: self.prepared_statements.clone(),
            transaction_id: self.transaction_id.clone(),
        }
    }
}
