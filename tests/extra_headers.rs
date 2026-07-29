//! Arbitrary extra headers ride on every request, but may not collide with a
//! header the client manages: `reqwest` appends rather than replaces, so a
//! collision would put two values on the wire instead of overriding one.
use std::collections::HashMap;

use serde_json::json;
use trino_rust_client::error::Error;
use trino_rust_client::{client::ClientBuilder, Row};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn mock_single_page() -> (MockServer, String, u16) {
    let server = MockServer::start().await;
    let stats = json!({
        "state": "FINISHED", "queued": false, "scheduled": true, "nodes": 1,
        "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1,
        "cpuTimeMillis": 0, "wallTimeMillis": 0, "queuedTimeMillis": 0,
        "elapsedTimeMillis": 0, "processedRows": 0, "processedBytes": 0,
        "peakMemoryBytes": 0, "spilledBytes": 0
    });
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "q", "infoUri": "http://localhost/ui",
            "columns": [{"name":"n","type":"integer",
                         "typeSignature":{"rawType":"integer","arguments":[]}}],
            "data": [[1]], "stats": stats, "warnings": []
        })))
        .mount(&server)
        .await;

    let uri = server.uri();
    let (host, port) = uri.trim_start_matches("http://").rsplit_once(':').unwrap();
    (server, host.to_string(), port.parse().unwrap())
}

#[tokio::test]
async fn extra_headers_are_sent() {
    let (server, host, port) = mock_single_page().await;
    let cli = ClientBuilder::new("u", host)
        .port(port)
        .extra_header("X-Gateway-Tenant", "acme")
        .build()
        .unwrap();
    cli.get_all::<Row>("SELECT 1").await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(
        reqs[0]
            .headers
            .get("x-gateway-tenant")
            .expect("configured extra header must be sent")
            .to_str()
            .unwrap(),
        "acme"
    );
}

#[tokio::test]
async fn extra_headers_replace_the_whole_map() {
    let (server, host, port) = mock_single_page().await;
    let mut m = HashMap::new();
    m.insert("X-A".to_string(), "1".to_string());
    let cli = ClientBuilder::new("u", host)
        .port(port)
        .extra_header("X-Dropped", "yes")
        .extra_headers(m)
        .build()
        .unwrap();
    cli.get_all::<Row>("SELECT 1").await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs[0].headers.get("x-a").unwrap().to_str().unwrap(), "1");
    assert!(reqs[0].headers.get("x-dropped").is_none());
}

#[test]
fn a_managed_header_is_rejected_at_build_time() {
    // Case-insensitively, and for a header the client sets itself.
    let err = ClientBuilder::new("u", "localhost")
        .extra_header("x-trino-user", "mallory")
        .build()
        .err()
        .expect("build must reject this header");
    assert!(
        matches!(&err, Error::ReservedHeader(name) if name == "x-trino-user"),
        "expected ReservedHeader, got {err:?}"
    );
}

#[test]
fn an_unmodelled_trino_header_is_allowed() {
    // The client does not send X-Trino-Something-New, so passing it through is
    // exactly the point of extra headers.
    ClientBuilder::new("u", "localhost")
        .extra_header("X-Trino-Something-New", "1")
        .build()
        .expect("an X-Trino-* header the client does not manage must be allowed");
}

#[test]
fn a_malformed_header_name_is_rejected_at_build_time() {
    let err = ClientBuilder::new("u", "localhost")
        .extra_header("bad header name", "v")
        .build()
        .err()
        .expect("build must reject this header");
    assert!(
        matches!(err, Error::InvalidHeader(_)),
        "expected InvalidHeader, got {err:?}"
    );
}

#[test]
fn a_malformed_header_value_is_rejected_at_build_time() {
    let err = ClientBuilder::new("u", "localhost")
        .extra_header("X-Ok", "bad\nvalue")
        .build()
        .err()
        .expect("build must reject this header");
    assert!(
        matches!(err, Error::InvalidHeader(_)),
        "expected InvalidHeader, got {err:?}"
    );
}
