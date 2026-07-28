use trino_rust_client::models::{QueryResult, QueryResultData};
use trino_rust_client::Trino;

#[derive(Trino, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct TestRecord {
    id: i64,
    name: String,
}

#[test]
fn test_spooled_data_deserialization() {
    let json = r#"{
        "id": "test-query-id",
        "infoUri": "http://localhost:8080/v1/query/test-query-id",
        "nextUri": null,
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "typeSignature": {
                    "rawType": "bigint",
                    "arguments": []
                }
            },
            {
                "name": "name",
                "type": "varchar",
                "typeSignature": {
                    "rawType": "varchar",
                    "arguments": []
                }
            }
        ],
        "data": {
            "encoding": "json",
            "segments": [
                {
                    "type": "inline",
                    "data": "W3siaWQiOjEsIm5hbWUiOiJhbGljZSJ9XQ==",
                    "metadata": {}
                },
                {
                    "type": "inline",
                    "data": "W3siaWQiOjIsIm5hbWUiOiJib2IifV0=",
                    "metadata": {}
                }
            ]
        },
        "stats": {
            "state": "FINISHED",
            "queued": false,
            "scheduled": true,
            "nodes": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 1,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 0,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 0,
            "processedRows": 2,
            "processedBytes": 0,
            "peakMemoryBytes": 0,
            "spilledBytes": 0
        },
        "warnings": []
    }"#;
    let result: QueryResult<TestRecord> = serde_json::from_str(json).unwrap();

    assert!(matches!(result.data, Some(QueryResultData::Spooled(_))));
    assert_eq!(result.columns.as_ref().unwrap().len(), 2);

    // Decoding spooled rows requires a Client (segment fetching) and the column
    // metadata; QueryResultData has neither, so it reports rather than guesses.
    // Task 4 adds the pager-based test that actually decodes these rows.
    assert!(matches!(
        result.data.unwrap().into_vec(),
        Err(trino_rust_client::error::Error::Protocol(_))
    ));
}

#[test]
fn test_direct_protocol_fallback() {
    let json = r#"{
        "id": "test-query-id",
        "infoUri": "http://localhost:8080/v1/query/test-query-id",
        "nextUri": null,
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "typeSignature": {
                    "rawType": "bigint",
                    "arguments": []
                }
            },
            {
                "name": "name",
                "type": "varchar",
                "typeSignature": {
                    "rawType": "varchar",
                    "arguments": []
                }
            }
        ],
        "data": [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"}
        ],
        "stats": {
            "state": "FINISHED",
            "queued": false,
            "scheduled": true,
            "nodes": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 1,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 0,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 0,
            "processedRows": 2,
            "processedBytes": 0,
            "peakMemoryBytes": 0,
            "spilledBytes": 0
        },
        "warnings": []
    }"#;
    let result: QueryResult<TestRecord> = serde_json::from_str(json).unwrap();

    assert!(matches!(result.data, Some(QueryResultData::Direct(_))));
    if let Some(QueryResultData::Direct(data)) = result.data {
        assert_eq!(data.len(), 2);
        assert_eq!(data[0].id, 1);
        assert_eq!(data[0].name, "alice");
    }
}

#[test]
fn test_mixed_protocol_detection() {
    let spooled_json = r#"{
        "id": "test-query-id",
        "infoUri": "http://localhost:8080/v1/query/test-query-id",
        "nextUri": null,
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "typeSignature": {
                    "rawType": "bigint",
                    "arguments": []
                }
            },
            {
                "name": "name",
                "type": "varchar",
                "typeSignature": {
                    "rawType": "varchar",
                    "arguments": []
                }
            }
        ],
        "data": {
            "encoding": "json",
            "segments": [
                {
                    "type": "inline",
                    "data": "W3siaWQiOjEsIm5hbWUiOiJhbGljZSJ9XQ==",
                    "metadata": {}
                }
            ]
        },
        "stats": {
            "state": "FINISHED",
            "queued": false,
            "scheduled": true,
            "nodes": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 1,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 0,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 0,
            "processedRows": 1,
            "processedBytes": 0,
            "peakMemoryBytes": 0,
            "spilledBytes": 0
        },
        "warnings": []
    }"#;
    let direct_json = r#"{
        "id": "test-query-id",
        "infoUri": "http://localhost:8080/v1/query/test-query-id",
        "nextUri": null,
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "typeSignature": {
                    "rawType": "bigint",
                    "arguments": []
                }
            },
            {
                "name": "name",
                "type": "varchar",
                "typeSignature": {
                    "rawType": "varchar",
                    "arguments": []
                }
            }
        ],
        "data": [
            {"id": 1, "name": "alice"}
        ],
        "stats": {
            "state": "FINISHED",
            "queued": false,
            "scheduled": true,
            "nodes": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 1,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 0,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 0,
            "processedRows": 1,
            "processedBytes": 0,
            "peakMemoryBytes": 0,
            "spilledBytes": 0
        },
        "warnings": []
    }"#;

    let spooled: QueryResult<TestRecord> = serde_json::from_str(spooled_json).unwrap();
    let direct: QueryResult<TestRecord> = serde_json::from_str(direct_json).unwrap();

    assert!(matches!(spooled.data, Some(QueryResultData::Spooled(_))));
    assert!(matches!(direct.data, Some(QueryResultData::Direct(_))));
}

/// Inline segments decode through the pager, which has the column metadata.
#[cfg(feature = "spooling")]
#[tokio::test]
async fn inline_segments_decode_through_pager() {
    use serde_json::json;
    use trino_rust_client::{client::ClientBuilder, spooling::SpoolingEncoding, Row};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let stats = json!({
        "state": "FINISHED", "queued": false, "scheduled": true, "nodes": 1,
        "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1,
        "cpuTimeMillis": 0, "wallTimeMillis": 0, "queuedTimeMillis": 0,
        "elapsedTimeMillis": 0, "processedRows": 2, "processedBytes": 0,
        "peakMemoryBytes": 0, "spilledBytes": 0
    });
    // base64 of `[[1],[2]]` — Trino's json encoding is positional.
    let inline = "W1sxXSxbMl1d";

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "q", "infoUri": "http://localhost/ui",
            "columns": [{"name":"n","type":"integer",
                         "typeSignature":{"rawType":"integer","arguments":[]}}],
            "data": {"encoding": "json", "segments": [
                {"type": "inline", "data": inline, "metadata": {}}
            ]},
            "stats": stats, "warnings": []
        })))
        .mount(&server)
        .await;

    let uri = server.uri();
    let (host, port) = uri.trim_start_matches("http://").rsplit_once(':').unwrap();
    let cli = ClientBuilder::new("u", host.to_string())
        .port(port.parse().unwrap())
        .spooling_encoding(SpoolingEncoding::Json)
        .build()
        .unwrap();

    let mut pager = cli.pager::<Row>("SELECT n FROM t").await.unwrap();
    let rows = pager.next_page().await.unwrap().expect("one page of rows");
    assert_eq!(rows.len(), 2);
}
