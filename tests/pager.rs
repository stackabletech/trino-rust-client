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
