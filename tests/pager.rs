//! `Client::pager` — manual, incremental paging that can resolve spooled data.
use serde_json::{json, Value};
use trino_rust_client::{client::ClientBuilder, Row};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub fn stats() -> Value {
    json!({
        "state": "FINISHED", "queued": false, "scheduled": true, "nodes": 1,
        "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1,
        "cpuTimeMillis": 0, "wallTimeMillis": 0, "queuedTimeMillis": 0,
        "elapsedTimeMillis": 0, "processedRows": 0, "processedBytes": 0,
        "peakMemoryBytes": 0, "spilledBytes": 0
    })
}

pub fn columns() -> Value {
    json!([{"name":"n","type":"integer",
            "typeSignature":{"rawType":"integer","arguments":[]}}])
}

pub async fn server_addr(server: &MockServer) -> (String, u16) {
    let uri = server.uri();
    let (host, port) = uri.trim_start_matches("http://").rsplit_once(':').unwrap();
    (host.to_string(), port.parse().unwrap())
}

async fn mount(server: &MockServer, verb: &str, p: &str, body: Value) {
    let m = if verb == "POST" {
        Mock::given(method("POST")).and(path("/v1/statement".to_string()))
    } else {
        Mock::given(method("GET")).and(path(p.to_string()))
    };
    m.respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

/// QUEUED (no columns, no data) -> 2 rows -> 1 row, finished.
async fn mount_direct_pages(server: &MockServer) {
    let uri = server.uri();
    mount(
        server,
        "POST",
        "",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/q/1"),
            "stats": stats(), "warnings": []
        }),
    )
    .await;
    mount(
        server,
        "GET",
        "/v1/statement/q/1",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/q/2"),
            "columns": columns(), "data": [[1],[2]],
            "stats": stats(), "warnings": []
        }),
    )
    .await;
    mount(
        server,
        "GET",
        "/v1/statement/q/2",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "columns": columns(), "data": [[3]],
            "stats": stats(), "warnings": []
        }),
    )
    .await;
}

#[tokio::test]
async fn pager_yields_pages_and_resolves_schema_up_front() {
    let server = MockServer::start().await;
    mount_direct_pages(&server).await;
    let (host, port) = server_addr(&server).await;
    let cli = ClientBuilder::new("u", host).port(port).build().unwrap();

    let mut pager = cli.pager::<Row>("SELECT n FROM t").await.unwrap();

    // Schema and id are known before any row is produced.
    assert_eq!(pager.query_id(), "q");
    assert_eq!(pager.columns().len(), 1);
    assert_eq!(pager.columns()[0].name, "n");

    let mut pages = Vec::new();
    while let Some(rows) = pager.next_page().await.unwrap() {
        pages.push(rows.len());
    }
    // The data-less QUEUED page is skipped, not surfaced as an empty page.
    assert_eq!(pages, vec![2, 1]);
}

#[tokio::test]
async fn pager_is_fused_after_exhaustion() {
    let server = MockServer::start().await;
    mount_direct_pages(&server).await;
    let (host, port) = server_addr(&server).await;
    let cli = ClientBuilder::new("u", host).port(port).build().unwrap();

    let mut pager = cli.pager::<Row>("SELECT n FROM t").await.unwrap();
    while pager.next_page().await.unwrap().is_some() {}
    assert!(pager.next_page().await.unwrap().is_none());
    assert!(pager.next_page().await.unwrap().is_none());
}

#[tokio::test]
async fn pager_cancel_issues_delete() {
    let server = MockServer::start().await;
    mount_direct_pages(&server).await;
    Mock::given(method("DELETE"))
        .and(path("/v1/query/q"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let (host, port) = server_addr(&server).await;
    let cli = ClientBuilder::new("u", host).port(port).build().unwrap();

    let mut pager = cli.pager::<Row>("SELECT n FROM t").await.unwrap();
    let _ = pager.next_page().await.unwrap();
    pager.cancel().await.expect("cancel should succeed");

    let reqs = server.received_requests().await.unwrap();
    let deletes = reqs
        .iter()
        .filter(|r| r.method == wiremock::http::Method::DELETE)
        .count();
    assert_eq!(deletes, 1, "cancel must fire exactly one DELETE");
}

// ---------------------------------------------------------------------------
// Spooled protocol
// ---------------------------------------------------------------------------

/// Two pages, each carrying one *remote* segment of 500 rows, served from a
/// separate "object storage" path. This is the shape that returned 0 rows.
#[cfg(feature = "spooling")]
#[tokio::test]
async fn pager_fetches_remote_segments_across_pages() {
    let server = MockServer::start().await;
    let uri = server.uri();

    // Trino's `json` spooling encoding emits positional arrays.
    let seg =
        |from: i64| -> Value { Value::Array((from..from + 500).map(|n| json!([n])).collect()) };

    mount(
        &server,
        "POST",
        "",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/q/1"),
            "columns": columns(),
            "data": {"encoding": "json", "segments": [
                {"type": "spooled", "uri": format!("{uri}/segments/0"),
                 "ackUri": format!("{uri}/ack/0"),
                 "metadata": {"rowsCount": 500, "segmentSize": 4096}}
            ]},
            "stats": stats(), "warnings": []
        }),
    )
    .await;

    // Second page deliberately omits `columns` — the pager must use the cached
    // metadata from page 1 to decode it.
    mount(
        &server,
        "GET",
        "/v1/statement/q/1",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "data": {"encoding": "json", "segments": [
                {"type": "spooled", "uri": format!("{uri}/segments/1"),
                 "metadata": {"rowsCount": 500, "segmentSize": 4096}}
            ]},
            "stats": stats(), "warnings": []
        }),
    )
    .await;

    for (i, from) in [(0, 1i64), (1, 501i64)] {
        Mock::given(method("GET"))
            .and(path(format!("/segments/{i}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(seg(from)))
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/ack/0"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let (host, port) = server_addr(&server).await;
    let cli = ClientBuilder::new("u", host)
        .port(port)
        .spooling_encoding(trino_rust_client::spooling::SpoolingEncoding::Json)
        .build()
        .unwrap();

    let mut pager = cli.pager::<Row>("SELECT n FROM t").await.unwrap();
    let mut total = 0usize;
    while let Some(rows) = pager.next_page().await.unwrap() {
        total += rows.len();
    }

    assert_eq!(total, 1000, "every spooled row must be delivered");
}

/// A query may switch protocols between pages; `get_all` used to reject this.
#[cfg(feature = "spooling")]
#[tokio::test]
async fn pager_handles_direct_and_spooled_in_one_query() {
    let server = MockServer::start().await;
    let uri = server.uri();

    mount(
        &server,
        "POST",
        "",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/q/1"),
            "columns": columns(), "data": [[1],[2]],
            "stats": stats(), "warnings": []
        }),
    )
    .await;
    mount(
        &server,
        "GET",
        "/v1/statement/q/1",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "columns": columns(),
            "data": {"encoding": "json", "segments": [
                {"type": "spooled", "uri": format!("{uri}/segments/0"), "metadata": {}}
            ]},
            "stats": stats(), "warnings": []
        }),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/segments/0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([[3], [4], [5]])))
        .mount(&server)
        .await;

    let (host, port) = server_addr(&server).await;
    let cli = ClientBuilder::new("u", host)
        .port(port)
        .spooling_encoding(trino_rust_client::spooling::SpoolingEncoding::Json)
        .build()
        .unwrap();

    let mut pager = cli.pager::<Row>("SELECT n FROM t").await.unwrap();
    let mut total = 0usize;
    while let Some(rows) = pager.next_page().await.unwrap() {
        total += rows.len();
    }
    assert_eq!(total, 5);
}

/// `get_all` used to reject a query whose pages switch protocol.
#[cfg(feature = "spooling")]
#[tokio::test]
async fn get_all_accepts_mixed_protocols() {
    let server = MockServer::start().await;
    let uri = server.uri();

    mount(
        &server,
        "POST",
        "",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "nextUri": format!("{uri}/v1/statement/q/1"),
            "columns": columns(), "data": [[1],[2]],
            "stats": stats(), "warnings": []
        }),
    )
    .await;
    mount(
        &server,
        "GET",
        "/v1/statement/q/1",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "columns": columns(),
            "data": {"encoding": "json", "segments": [
                {"type": "spooled", "uri": format!("{uri}/segments/0"), "metadata": {}}
            ]},
            "stats": stats(), "warnings": []
        }),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/segments/0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([[3], [4], [5]])))
        .mount(&server)
        .await;

    let (host, port) = server_addr(&server).await;
    let cli = ClientBuilder::new("u", host)
        .port(port)
        .spooling_encoding(trino_rust_client::spooling::SpoolingEncoding::Json)
        .build()
        .unwrap();

    let ds = cli.get_all::<Row>("SELECT n FROM t").await.unwrap();
    assert_eq!(ds.len(), 5);
}

#[cfg(feature = "spooling")]
async fn mount_single_spooled_page(server: &MockServer) {
    let uri = server.uri();
    mount(
        server,
        "POST",
        "",
        json!({
            "id": "q", "infoUri": format!("{uri}/ui"),
            "columns": columns(),
            "data": {"encoding": "json", "segments": [
                {"type": "spooled", "uri": format!("{uri}/segments/0"), "metadata": {}}
            ]},
            "stats": stats(), "warnings": []
        }),
    )
    .await;
}

#[cfg(feature = "spooling")]
fn fast_retry_client(host: String, port: u16) -> trino_rust_client::client::Client {
    use std::time::Duration;
    use trino_rust_client::retry::RetryPolicy;
    ClientBuilder::new("u", host)
        .port(port)
        .spooling_encoding(trino_rust_client::spooling::SpoolingEncoding::Json)
        .retry_policy(RetryPolicy {
            max_retries: 3,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            jitter: false,
        })
        .build()
        .unwrap()
}

/// A transient 503 from object storage must not fail the page.
#[cfg(feature = "spooling")]
#[tokio::test]
async fn segment_fetch_retries_transient_failure() {
    let server = MockServer::start().await;
    mount_single_spooled_page(&server).await;

    Mock::given(method("GET"))
        .and(path("/segments/0"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/segments/0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([[1], [2]])))
        .mount(&server)
        .await;

    let (host, port) = server_addr(&server).await;
    let cli = fast_retry_client(host, port);
    let mut pager = cli.pager::<Row>("SELECT n FROM t").await.unwrap();

    let rows = pager.next_page().await.expect("503 must be retried");
    assert_eq!(rows.unwrap().len(), 2);

    let attempts = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/segments/0")
        .count();
    assert_eq!(attempts, 2, "one failure then one success");
}

/// An expired presigned URL is terminal — retrying it just wastes time.
#[cfg(feature = "spooling")]
#[tokio::test]
async fn segment_fetch_does_not_retry_forbidden() {
    let server = MockServer::start().await;
    mount_single_spooled_page(&server).await;
    Mock::given(method("GET"))
        .and(path("/segments/0"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;

    let (host, port) = server_addr(&server).await;
    let cli = fast_retry_client(host, port);
    let mut pager = cli.pager::<Row>("SELECT n FROM t").await.unwrap();

    assert!(pager.next_page().await.is_err(), "403 must fail the page");

    let attempts = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/segments/0")
        .count();
    assert_eq!(attempts, 1, "403 must not be retried");
}
