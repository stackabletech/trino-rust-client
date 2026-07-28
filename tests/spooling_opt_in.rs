//! The spooling protocol must be advertised only when explicitly requested:
//! Cargo features are additive, so compiling `spooling` (possibly because some
//! other crate in the graph enabled it) must not change the wire protocol.
use serde_json::json;
use trino_rust_client::{client::ClientBuilder, Row};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const HEADER: &str = "x-trino-query-data-encoding";

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
    let (h, p) = (host.to_string(), port.parse().unwrap());
    (server, h, p)
}

#[tokio::test]
async fn spooling_header_absent_by_default() {
    let (server, host, port) = mock_single_page().await;
    let cli = ClientBuilder::new("u", host).port(port).build().unwrap();
    cli.get_all::<Row>("SELECT 1").await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    assert!(
        reqs.iter().all(|r| r.headers.get(HEADER).is_none()),
        "spooling must not be advertised without an explicit opt-in"
    );
}

#[cfg(feature = "spooling")]
#[tokio::test]
async fn spooling_header_sent_when_requested() {
    use trino_rust_client::spooling::SpoolingEncoding;

    let (server, host, port) = mock_single_page().await;
    let cli = ClientBuilder::new("u", host)
        .port(port)
        .spooling_encoding(SpoolingEncoding::JsonLz4)
        .build()
        .unwrap();
    cli.get_all::<Row>("SELECT 1").await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    let submit = &reqs[0];
    assert_eq!(
        submit.headers.get(HEADER).unwrap().to_str().unwrap(),
        "json+lz4",
        "the requested encoding must be sent verbatim"
    );
}
