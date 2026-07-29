//! HTTP proxy configuration for coordinator and segment traffic.
use reqwest::Url;

use crate::error::{Error, Result};

/// An HTTP or HTTPS proxy, applied to every request the client makes.
///
/// Set it with [`ClientBuilder::proxy`](crate::client::ClientBuilder::proxy).
#[derive(Clone, Debug)]
pub struct Proxy {
    pub(crate) url: Url,
    pub(crate) basic_auth: Option<(String, String)>,
}

impl Proxy {
    /// Route all requests through `url`.
    ///
    /// Only `http` and `https` are supported. SOCKS would need `reqwest`'s
    /// `socks` feature, which this crate does not enable, so a `socks5://` URL
    /// is rejected here rather than failing later at connect time.
    pub fn all(url: impl AsRef<str>) -> Result<Self> {
        let parsed = Url::parse(url.as_ref())
            .map_err(|e| Error::InvalidProxy(format!("{}: {e}", url.as_ref())))?;
        match parsed.scheme() {
            "http" | "https" => {}
            other => {
                return Err(Error::InvalidProxy(format!(
                    "unsupported proxy scheme `{other}`; only http and https are supported"
                )))
            }
        }
        Ok(Self {
            url: parsed,
            basic_auth: None,
        })
    }

    /// Authenticate to the proxy with HTTP Basic credentials.
    pub fn basic_auth(mut self, username: impl ToString, password: impl ToString) -> Self {
        self.basic_auth = Some((username.to_string(), password.to_string()));
        self
    }

    pub(crate) fn to_reqwest(&self) -> Result<reqwest::Proxy> {
        let mut proxy = reqwest::Proxy::all(self.url.as_str())?;
        if let Some((user, password)) = &self.basic_auth {
            proxy = proxy.basic_auth(user, password);
        }
        Ok(proxy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn http_and_https_are_accepted() {
        assert!(Proxy::all("http://proxy.example.com:3128").is_ok());
        assert!(Proxy::all("https://proxy.example.com:3128").is_ok());
    }

    #[test]
    fn socks_is_rejected_up_front() {
        // reqwest's `socks` feature is not enabled, so a SOCKS URL would be
        // accepted here and then fail obscurely at connect time.
        let err = Proxy::all("socks5://proxy.example.com:1080").unwrap_err();
        assert!(
            matches!(&err, Error::InvalidProxy(m) if m.contains("socks5")),
            "expected InvalidProxy naming the scheme, got {err:?}"
        );
    }

    #[test]
    fn a_malformed_url_is_rejected() {
        let err = Proxy::all("not a url").unwrap_err();
        assert!(
            matches!(err, Error::InvalidProxy(_)),
            "expected InvalidProxy, got {err:?}"
        );
    }

    #[test]
    fn basic_auth_is_retained() {
        let p = Proxy::all("http://proxy.example.com:3128")
            .unwrap()
            .basic_auth("u", "p");
        assert!(p.to_reqwest().is_ok());
    }
}
