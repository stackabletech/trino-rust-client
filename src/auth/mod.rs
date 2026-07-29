use std::fmt;
use std::sync::Arc;
use std::time::Duration;

mod oauth2;
pub(crate) use oauth2::run_flow;
pub use oauth2::{
    parse_www_authenticate, BrowserRedirectHandler, Challenge, OAuth2Config, OAuth2State,
    RedirectHandler,
};

pub(crate) const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(300);
pub(crate) const DEFAULT_MIN_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
#[non_exhaustive]
pub enum Auth {
    Basic(String, Option<String>),
    Jwt(String),
    OAuth2(Arc<OAuth2State>),
}

impl Auth {
    pub fn new_basic(username: impl ToString, password: Option<impl ToString>) -> Auth {
        Auth::Basic(username.to_string(), password.map(|p| p.to_string()))
    }

    pub fn new_jwt(token: impl ToString) -> Auth {
        Auth::Jwt(token.to_string())
    }

    /// Interactive OAuth2 using the default browser handler and defaults.
    ///
    /// # Token caching
    ///
    /// The bearer token lives in the returned `Auth`, not in a global cache.
    /// Clone one `Auth` across several clients and they share a single login;
    /// call this a second time and the user is prompted a second time.
    ///
    /// ```no_run
    /// # use trino_rust_client::auth::Auth;
    /// # use trino_rust_client::ClientBuilder;
    /// # fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// // One login, shared by both clients.
    /// let auth = Auth::new_oauth2();
    /// let reads = ClientBuilder::new("u", "trino").secure(true).auth(auth.clone()).build()?;
    /// let writes = ClientBuilder::new("u", "trino").secure(true).auth(auth).build()?;
    /// # Ok(()) }
    /// ```
    pub fn new_oauth2() -> Auth {
        Auth::new_oauth2_with_config(OAuth2Config::default())
    }

    /// Interactive OAuth2 with a caller-supplied redirect handler.
    pub fn new_oauth2_with_handler(handler: Arc<dyn RedirectHandler>) -> Auth {
        Auth::new_oauth2_with_config(OAuth2Config {
            handler,
            ..Default::default()
        })
    }

    /// Interactive OAuth2 with full control over the flow.
    pub fn new_oauth2_with_config(config: OAuth2Config) -> Auth {
        Auth::OAuth2(Arc::new(OAuth2State::new(
            config.handler,
            config.poll_timeout,
            config.min_poll_interval,
        )))
    }
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Auth::Basic(name, _) => f
                .debug_struct("BasicAuth")
                .field("username", name)
                .field("password", &"******")
                .finish(),

            Auth::Jwt(_) => f.debug_struct("JwtAuth").field("token", &"******").finish(),

            Auth::OAuth2(_) => f
                .debug_struct("OAuth2Auth")
                .field("token", &"******")
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth2_debug_redacts_token() {
        let auth = Auth::new_oauth2();
        if let Auth::OAuth2(state) = &auth {
            *state.token.write().unwrap() = Some("super-secret".to_string());
        }
        let dbg = format!("{auth:?}");
        assert!(!dbg.contains("super-secret"), "token leaked: {dbg}");
        assert!(dbg.contains("OAuth2Auth"));
    }

    #[test]
    fn config_is_honoured_and_token_survives_cloning() {
        use std::time::Duration;

        let auth = Auth::new_oauth2_with_config(OAuth2Config {
            poll_timeout: Duration::from_secs(42),
            min_poll_interval: Duration::from_millis(250),
            ..Default::default()
        });

        let Auth::OAuth2(state) = &auth else {
            panic!("expected OAuth2 auth")
        };
        assert_eq!(state.poll_timeout, Duration::from_secs(42));
        assert_eq!(state.min_poll_interval, Duration::from_millis(250));

        // Cloning the Auth shares one token cache — this is what the removed
        // `with_poll` broke by rebuilding the state behind a new Arc.
        *state.token.write().unwrap() = Some("tok".to_string());
        let cloned = auth.clone();
        let Auth::OAuth2(cloned_state) = &cloned else {
            panic!("expected OAuth2 auth")
        };
        assert_eq!(cloned_state.cached_token().as_deref(), Some("tok"));
    }

    #[test]
    fn default_config_uses_the_documented_defaults() {
        let config = OAuth2Config::default();
        assert_eq!(config.poll_timeout, DEFAULT_POLL_TIMEOUT);
        assert_eq!(config.min_poll_interval, DEFAULT_MIN_POLL_INTERVAL);
    }
}
