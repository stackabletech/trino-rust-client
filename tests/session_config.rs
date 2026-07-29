//! Session configuration that `ClientBuilder` can set must actually reach the
//! wire. Each test asserts the exact header value the coordinator receives.
use serde_json::json;
use trino_rust_client::selected_role::{RoleType, SelectedRole};
use trino_rust_client::{client::ClientBuilder, Row, Tz};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A coordinator that answers one `POST /v1/statement` with a complete,
/// single-page result — enough for `get_all` to finish so the request headers
/// can be inspected.
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
async fn timezone_is_sent() {
    let (server, host, port) = mock_single_page().await;
    let cli = ClientBuilder::new("u", host)
        .port(port)
        .timezone("Europe/Berlin".parse::<Tz>().unwrap())
        .build()
        .unwrap();
    cli.get_all::<Row>("SELECT 1").await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(
        reqs[0]
            .headers
            .get("x-trino-time-zone")
            .expect("X-Trino-Time-Zone must be sent when a timezone is configured")
            .to_str()
            .unwrap(),
        "Europe/Berlin"
    );
}

#[tokio::test]
async fn timezone_is_absent_by_default() {
    let (server, host, port) = mock_single_page().await;
    let cli = ClientBuilder::new("u", host).port(port).build().unwrap();
    cli.get_all::<Row>("SELECT 1").await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    assert!(
        reqs[0].headers.get("x-trino-time-zone").is_none(),
        "without a configured timezone the coordinator default must apply"
    );
}

#[tokio::test]
async fn role_is_sent_per_catalog() {
    let (server, host, port) = mock_single_page().await;
    let cli = ClientBuilder::new("u", host)
        .port(port)
        .role(
            "hive",
            SelectedRole::new(RoleType::Role, Some("admin".into())),
        )
        .build()
        .unwrap();
    cli.get_all::<Row>("SELECT 1").await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    // `add_header_map` form-urlencodes each entry, so the braces are escaped.
    // Whether a real coordinator accepts this encoding on the *request* side is
    // verified separately — see Task 10, Step 1.
    assert_eq!(
        reqs[0]
            .headers
            .get("x-trino-role")
            .expect("X-Trino-Role must be sent when a role is configured")
            .to_str()
            .unwrap(),
        "hive=ROLE%7Badmin%7D"
    );
}
