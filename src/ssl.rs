use std::io::Read;
use std::path::Path;

use crate::error::{Error, Result};

/// How strictly the coordinator's TLS certificate is checked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum TlsVerification {
    /// Verify the certificate chain and that the hostname matches. The default.
    #[default]
    Full,
    /// Verify the certificate chain but not the hostname, for a coordinator
    /// reached under a name its certificate does not carry.
    ///
    /// **Requires a root certificate** set with [`Ssl::root_cert`]: rustls
    /// only permits skipping hostname verification when the trust store is
    /// supplied explicitly, which excludes the platform's own roots. Building
    /// a client in this mode without one is an [`Error::Tls`].
    CaOnly,
    /// Verify nothing — any certificate is accepted. Test environments only.
    None,
}

/// A trusted root certificate. Load one with [`Ssl::read_pem`] or
/// [`Ssl::read_der`].
#[derive(Clone, Debug)]
pub struct Certificate(pub(crate) reqwest::Certificate);

/// A client certificate and its private key, for mutual-TLS authentication.
#[derive(Clone, Debug)]
pub struct Identity(pub(crate) reqwest::Identity);

impl Identity {
    /// Load an identity from a PEM buffer holding the certificate chain
    /// followed by a PKCS#8 private key.
    ///
    /// PEM is the only supported format: this crate builds `reqwest` on
    /// rustls, which accepts neither PKCS#12 nor JKS.
    pub fn from_pem(pem: &[u8]) -> Result<Self> {
        reqwest::Identity::from_pem(pem)
            .map(Identity)
            .map_err(|e| Error::Tls(format!("Cannot load PEM identity {:?}", e)))
    }

    /// Read a PEM identity from `path`. See [`Identity::from_pem`].
    pub fn read_pem<P: AsRef<Path>>(path: &P) -> Result<Self> {
        Self::from_pem(&read_file(path)?)
    }
}

/// TLS material for the connection: a trusted root and/or a client identity.
///
/// ```no_run
/// # use trino_rust_client::ssl::{Identity, Ssl};
/// # fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let ssl = Ssl::new()
///     .root_cert(Ssl::read_pem(&"/path/root.pem")?)
///     .identity(Identity::read_pem(&"/path/client.pem")?);
/// # Ok(()) }
/// ```
#[derive(Clone, Debug, Default)]
pub struct Ssl {
    pub(crate) root_cert: Option<Certificate>,
    pub(crate) identity: Option<Identity>,
}

impl Ssl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Trust `cert`, and only `cert`, as the root of the coordinator's chain.
    ///
    /// It replaces the platform's roots rather than joining them: naming a
    /// private CA is a pin, and a pin that still accepted every public CA
    /// would not be one. A coordinator with a publicly issued certificate
    /// needs no root certificate at all.
    pub fn root_cert(mut self, cert: Certificate) -> Self {
        self.root_cert = Some(cert);
        self
    }

    /// Present `identity` as the client certificate (mutual TLS).
    pub fn identity(mut self, identity: Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    pub fn read_pem<P: AsRef<Path>>(root_certificate_path: &P) -> Result<Certificate> {
        let buf = read_file(root_certificate_path)?;
        match reqwest::Certificate::from_pem(&buf) {
            Ok(cert) => Ok(Certificate(cert)),
            Err(e) => Err(Error::Tls(format!("Cannot load PEM certificate {:?}", e))),
        }
    }

    pub fn read_der<P: AsRef<Path>>(root_certificate_path: &P) -> Result<Certificate> {
        let buf = read_file(root_certificate_path)?;
        match reqwest::Certificate::from_der(&buf) {
            Ok(cert) => Ok(Certificate(cert)),
            Err(e) => Err(Error::Tls(format!("Cannot load DER certificate {:?}", e))),
        }
    }
}

fn read_file<P: AsRef<Path>>(file_path: &P) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    std::fs::File::open(file_path)
        .map_err(|e| {
            Error::Tls(format!(
                "Error opening file {}. {}",
                file_path.as_ref().display(),
                e
            ))
        })?
        .read_to_end(&mut buf)
        .map_err(|e| {
            Error::Tls(format!(
                "Error reading file {}. {}",
                file_path.as_ref().display(),
                e
            ))
        })?;

    Ok(buf)
}
