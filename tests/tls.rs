//! TLS configuration. The verification modes themselves cannot be exercised
//! here — `wiremock` serves plain HTTP — so those live in an `#[ignore]`d test
//! that needs a real HTTPS coordinator. What is testable is that the API
//! composes and that malformed material is rejected.
use trino_rust_client::error::Error;
use trino_rust_client::ssl::{Identity, Ssl};
use trino_rust_client::{client::ClientBuilder, TlsVerification};

#[test]
fn full_verification_is_the_default() {
    assert_eq!(TlsVerification::default(), TlsVerification::Full);
}

/// A self-signed certificate that is never presented by anything — it exists
/// only to give `CaOnly` a trust store to point at.
const TEST_CA: &str = "tests/data/tls/test_ca.pem";

#[test]
fn full_and_none_build_without_a_certificate() {
    for mode in [TlsVerification::Full, TlsVerification::None] {
        ClientBuilder::new("u", "localhost")
            .secure(true)
            .tls_verification(mode)
            .build()
            .unwrap_or_else(|e| panic!("{mode:?} must build, got {e:?}"));
    }
}

#[test]
fn ca_only_builds_with_a_root_certificate() {
    ClientBuilder::new("u", "localhost")
        .secure(true)
        .ssl(Ssl::new().root_cert(Ssl::read_pem(&TEST_CA).unwrap()))
        .tls_verification(TlsVerification::CaOnly)
        .build()
        .expect("CaOnly with a root certificate must build");
}

#[test]
fn ca_only_without_a_root_certificate_is_rejected() {
    // rustls only permits skipping hostname verification when the trust store
    // is supplied explicitly, and doing so excludes the platform roots — so
    // CaOnly without a certificate would trust nothing at all. Say so at build
    // time rather than failing every connection later.
    let err = ClientBuilder::new("u", "localhost")
        .secure(true)
        .tls_verification(TlsVerification::CaOnly)
        .build()
        .err()
        .expect("CaOnly without a root certificate must be rejected");
    assert!(
        matches!(&err, Error::Tls(m) if m.contains("root certificate")),
        "expected Error::Tls naming the requirement, got {err:?}"
    );
}

#[test]
fn full_builds_with_a_root_certificate() {
    // Full with a named root makes the same `tls_certs_only` call CaOnly does,
    // so this covers that the composition is accepted. What the trust store
    // then holds is not observable from the built client; that is
    // `a_named_root_replaces_the_platform_roots` below.
    ClientBuilder::new("u", "localhost")
        .secure(true)
        .ssl(Ssl::new().root_cert(Ssl::read_pem(&TEST_CA).unwrap()))
        .tls_verification(TlsVerification::Full)
        .build()
        .expect("Full with a root certificate must build");
}

#[test]
fn a_malformed_identity_is_rejected() {
    let err = Identity::from_pem(b"not a pem").unwrap_err();
    assert!(
        matches!(err, Error::Tls(_)),
        "expected Error::Tls, got {err:?}"
    );
}

#[test]
fn an_empty_ssl_builds() {
    // Both root_cert and identity are optional and independent.
    ClientBuilder::new("u", "localhost")
        .secure(true)
        .ssl(Ssl::new())
        .build()
        .expect("an Ssl with neither a root cert nor an identity must build");
}

#[test]
fn a_missing_certificate_file_is_reported() {
    let err = Ssl::read_pem(&"/nonexistent/root.pem").unwrap_err();
    assert!(
        matches!(err, Error::Tls(_)),
        "expected Error::Tls, got {err:?}"
    );
}

/// Requires a real HTTPS coordinator whose certificate does **not** match the
/// hostname it is reached on — that mismatch is the whole point of `CaOnly`.
///
/// The bundled OAuth2 stack works, with one wrinkle: its self-signed
/// certificate is `CN=localhost` with
/// `SAN=DNS:localhost,DNS:coordinator,IP:127.0.0.1`, so reaching it on
/// `localhost` *passes* hostname verification. Reach it on a name that is not
/// in that SAN list instead:
///
/// ```bash
/// docker compose -f integration_tests/test_setup/oauth/docker-compose.yml up -d
///
/// # One-time: a name the certificate does not cover.
/// echo "127.0.0.1 trino-mismatch.test" | sudo tee -a /etc/hosts
///
/// # The certificate is self-signed, so the server cert is its own CA.
/// openssl s_client -connect localhost:8443 -showcerts </dev/null 2>/dev/null \
///     | openssl x509 -outform pem > /tmp/trino-self-signed.pem
///
/// TRINO_TLS_HOST=trino-mismatch.test TRINO_TLS_PORT=8443 \
///     TRINO_TLS_CA=/tmp/trino-self-signed.pem \
///     cargo test --test tls -- --ignored ca_only_skips_hostname_verification
/// ```
#[tokio::test]
#[ignore = "needs a real HTTPS coordinator; see the doc comment"]
async fn ca_only_skips_hostname_verification() {
    use trino_rust_client::Row;

    let host = std::env::var("TRINO_TLS_HOST").expect("TRINO_TLS_HOST");
    let port: u16 = std::env::var("TRINO_TLS_PORT")
        .expect("TRINO_TLS_PORT")
        .parse()
        .expect("TRINO_TLS_PORT must be a u16");
    let ca = std::env::var("TRINO_TLS_CA").expect("TRINO_TLS_CA");

    // Full verification must fail on the hostname mismatch...
    let strict = ClientBuilder::new("u", &host)
        .port(port)
        .secure(true)
        .ssl(Ssl::new().root_cert(Ssl::read_pem(&ca).unwrap()))
        .tls_verification(TlsVerification::Full)
        .build()
        .unwrap();
    assert!(
        strict.get_all::<Row>("SELECT 1").await.is_err(),
        "Full must reject a hostname mismatch"
    );

    // ...and CaOnly must accept it while still checking the chain.
    let relaxed = ClientBuilder::new("u", &host)
        .port(port)
        .secure(true)
        .ssl(Ssl::new().root_cert(Ssl::read_pem(&ca).unwrap()))
        .tls_verification(TlsVerification::CaOnly)
        .build()
        .unwrap();
    relaxed
        .get_all::<Row>("SELECT 1")
        .await
        .expect("CaOnly must accept a hostname mismatch with a trusted chain");
}

/// A named root is the whole trust store, so a chain the *platform* trusts is
/// refused when the caller named something else. Verifying that needs a
/// coordinator whose CA is in the platform store, which no unit test may
/// arrange: installing a CA machine-wide is not a thing a test does to the
/// machine it runs on.
///
/// The Windows VM in the `stackable-odbc-trino` harness is already in that
/// state, because its OAuth2 browser login needs the test CA in
/// `Cert:\LocalMachine\Root`. That is where this is exercised for real, by
/// that repository's `integration-tests/suites/test_tls.py`. To drive it from
/// here:
///
/// ```bash
/// # A CA the platform trusts, and a coordinator it signed.
/// sudo cp integration-tests/generated/certs/ca.crt \
///     /usr/local/share/ca-certificates/trino-test-ca.crt
/// sudo update-ca-certificates
///
/// # TRINO_TLS_UNRELATED is a leaf that CA signed, so it is a valid
/// # certificate but not a trust anchor: it signed nothing.
/// TRINO_TLS_HOST=localhost TRINO_TLS_PORT=8443 \
///     TRINO_TLS_UNRELATED=integration-tests/generated/certs/keycloak.crt \
///     cargo test --test tls -- --ignored a_named_root_replaces_the_platform_roots
/// ```
#[tokio::test]
#[ignore = "needs a coordinator whose CA is in the platform trust store; see the doc comment"]
async fn a_named_root_replaces_the_platform_roots() {
    use trino_rust_client::Row;

    let host = std::env::var("TRINO_TLS_HOST").expect("TRINO_TLS_HOST");
    let port: u16 = std::env::var("TRINO_TLS_PORT")
        .expect("TRINO_TLS_PORT")
        .parse()
        .expect("TRINO_TLS_PORT must be a u16");
    let unrelated = std::env::var("TRINO_TLS_UNRELATED").expect("TRINO_TLS_UNRELATED");

    // No root named: the platform's store applies, and it trusts this chain.
    let platform = ClientBuilder::new("u", &host)
        .port(port)
        .secure(true)
        .tls_verification(TlsVerification::Full)
        .build()
        .unwrap();
    platform
        .get_all::<Row>("SELECT 1")
        .await
        .expect("the platform store must trust this coordinator, or the test proves nothing");

    // The same chain, with a certificate named that did not sign it. Falling
    // back to the platform store here is exactly what must not happen.
    let pinned = ClientBuilder::new("u", &host)
        .port(port)
        .secure(true)
        .ssl(Ssl::new().root_cert(Ssl::read_pem(&unrelated).unwrap()))
        .tls_verification(TlsVerification::Full)
        .build()
        .unwrap();
    assert!(
        pinned.get_all::<Row>("SELECT 1").await.is_err(),
        "Full accepted a chain the named root did not sign, so the platform's \
         roots were still in the trust store"
    );
}
