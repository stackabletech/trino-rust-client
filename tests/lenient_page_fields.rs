//! A coordinator that omits the informational fields of a page must still
//! deliver that page's rows.
//!
//! `stats` and `warnings` are reported for observability and read by nobody, so
//! a producer that leaves one out, a gateway, a proxy, a test double, or a
//! Trino release that renames a counter, must not fail the query. The unit
//! tests beside `Stat` and `QueryResult` cover the decode; these cover the
//! whole `get_all` path, which is where the omission actually bit.

use trino_rust_client::{client::ClientBuilder, Row};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn mock_server() -> (MockServer, String, u16) {
    let server = MockServer::start().await;
    let uri = server.uri();
    let host_port = uri.trim_start_matches("http://");
    let (host, port_str) = host_port.rsplit_once(':').unwrap();
    (server, host.to_string(), port_str.parse().unwrap())
}

/// One complete page carrying a single row, with `stats` and `warnings`
/// included or omitted per the flags.
fn page(with_stats: bool, with_warnings: bool) -> String {
    let stats = if with_stats {
        r#""stats": {"state": "FINISHED", "queued": false, "scheduled": false,
            "nodes": 1, "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0,
            "completedSplits": 1, "cpuTimeMillis": 1, "wallTimeMillis": 1,
            "queuedTimeMillis": 0, "elapsedTimeMillis": 1, "processedRows": 1,
            "processedBytes": 1, "peakMemoryBytes": 1, "spilledBytes": 0},"#
    } else {
        ""
    };
    let warnings = if with_warnings {
        r#""warnings": [],"#
    } else {
        ""
    };
    format!(
        r#"{{
            "id": "lenient_00001",
            "infoUri": "http://coordinator/ui/query.html",
            {stats}
            {warnings}
            "columns": [{{"name": "c", "type": "varchar",
                "typeSignature": {{"rawType": "varchar",
                    "arguments": [{{"kind": "LONG", "value": 2147483647}}]}}}}],
            "data": [["hello"]]
        }}"#
    )
}

async fn rows_from(body: String) -> Vec<Row> {
    let (server, host, port) = mock_server().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(&server)
        .await;

    let client = ClientBuilder::new("test_user", host)
        .port(port)
        .build()
        .unwrap();
    let dataset = client
        .get_all::<Row>("SELECT c FROM t")
        .await
        .expect("a page missing only informational fields still delivers rows");
    server.verify().await;
    dataset.split().1
}

#[tokio::test]
async fn a_page_without_warnings_still_delivers_its_rows() {
    let rows = rows_from(page(true, false)).await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn a_page_without_stats_still_delivers_its_rows() {
    let rows = rows_from(page(false, true)).await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn a_page_without_either_still_delivers_its_rows() {
    let rows = rows_from(page(false, false)).await;
    assert_eq!(rows.len(), 1);
}

/// The control: the same page with both present must be unaffected.
#[tokio::test]
async fn a_page_with_both_is_unchanged() {
    let rows = rows_from(page(true, true)).await;
    assert_eq!(rows.len(), 1);
}
