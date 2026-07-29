//! `USE`, `SET SESSION` and `SET ROLE` change server-side session state, which
//! the coordinator reports back on the response. The client tracks it; this is
//! the test that a caller can read the result.
use serde_json::json;
use trino_rust_client::selected_role::RoleType;
use trino_rust_client::{client::ClientBuilder, Row};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn snapshot_reflects_server_side_session_changes() {
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
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("X-Trino-Set-Catalog", "postgresql")
                .insert_header("X-Trino-Set-Schema", "public")
                .insert_header("X-Trino-Set-Path", "postgresql.public")
                .insert_header("X-Trino-Set-Session", "query_max_run_time=1h")
                .insert_header("X-Trino-Set-Role", "hive=ROLE%7Badmin%7D")
                .set_body_json(json!({
                    "id": "q", "infoUri": "http://localhost/ui",
                    "columns": [{"name":"n","type":"integer",
                                 "typeSignature":{"rawType":"integer","arguments":[]}}],
                    "data": [[1]], "stats": stats, "warnings": []
                })),
        )
        .mount(&server)
        .await;

    let uri = server.uri();
    let (host, port) = uri.trim_start_matches("http://").rsplit_once(':').unwrap();
    let cli = ClientBuilder::new("u", host)
        .port(port.parse::<u16>().unwrap())
        .catalog("tpch")
        .schema("sf1")
        .build()
        .unwrap();

    // Before the query the snapshot is the configured state.
    let before = cli.session_snapshot().await;
    assert_eq!(before.catalog.as_deref(), Some("tpch"));
    assert_eq!(before.schema.as_deref(), Some("sf1"));

    cli.get_all::<Row>("USE postgresql.public").await.unwrap();

    let after = cli.session_snapshot().await;
    assert_eq!(after.catalog.as_deref(), Some("postgresql"));
    assert_eq!(after.schema.as_deref(), Some("public"));
    assert_eq!(after.path.as_deref(), Some("postgresql.public"));
    assert_eq!(
        after
            .properties
            .get("query_max_run_time")
            .map(String::as_str),
        Some("1h")
    );
    let role = after.roles.get("hive").expect("role must be tracked");
    assert_eq!(role.ty, RoleType::Role);
    assert_eq!(role.role.as_deref(), Some("admin"));
}
