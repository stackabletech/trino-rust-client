//! `Client::decode_page` — the one decoder both the pager and an external
//! consumer with its own paging loop use.
use serde_json::{json, Value};
use trino_rust_client::client::ClientBuilder;
use trino_rust_client::models::QueryResultData;
use trino_rust_client::Row;
#[cfg(feature = "spooling")]
use wiremock::matchers::{method, path};
#[cfg(feature = "spooling")]
use wiremock::{Mock, MockServer, ResponseTemplate};

#[cfg(feature = "spooling")]
fn columns() -> Value {
    json!([{"name":"n","type":"integer",
            "typeSignature":{"rawType":"integer","arguments":[]}}])
}

/// A Direct page needs no server and no column metadata: its rows are already
/// decoded on the page.
#[tokio::test]
async fn a_direct_page_yields_its_inline_rows() {
    let cli = ClientBuilder::new("u", "localhost")
        .port(8080)
        .build()
        .unwrap();
    let data: QueryResultData<Row> = serde_json::from_value(json!([[1], [2], [3]])).unwrap();

    let rows = cli.decode_page(data, None).await.unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].clone().into_json(), vec![json!(1)]);
}

/// A spooled page's segments are fetched, decoded against the column metadata
/// and acknowledged.
#[cfg(feature = "spooling")]
#[tokio::test]
async fn a_spooled_page_fetches_and_decodes_its_segments() {
    let server = MockServer::start().await;
    let uri = server.uri();

    // Trino's `json` spooling encoding emits positional arrays.
    let segment: Value = Value::Array((1..=500).map(|n| json!([n])).collect());
    Mock::given(method("GET"))
        .and(path("/segments/0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(segment))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/ack/0"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let (host, port) = {
        let bare = uri.trim_start_matches("http://");
        let (h, p) = bare.rsplit_once(':').unwrap();
        (h.to_string(), p.parse::<u16>().unwrap())
    };
    let cli = ClientBuilder::new("u", host).port(port).build().unwrap();

    let data: QueryResultData<Row> = serde_json::from_value(json!({
        "encoding": "json",
        "segments": [
            {"type": "spooled", "uri": format!("{uri}/segments/0"),
             "ackUri": format!("{uri}/ack/0"),
             "metadata": {"rowsCount": 500, "segmentSize": 4096}}
        ]
    }))
    .unwrap();
    let cols: Vec<trino_rust_client::models::Column> = serde_json::from_value(columns()).unwrap();

    let rows = cli.decode_page(data, Some(&cols)).await.unwrap();

    assert_eq!(rows.len(), 500, "every spooled row must be delivered");
    // `expect(1)` is verified here: an unacknowledged segment lives until the
    // coordinator's `fs.segment.ttl`.
    server.verify().await;
}

/// Column metadata is what a spooled segment is decoded against, so its absence
/// is an error rather than an empty page.
#[cfg(feature = "spooling")]
#[tokio::test]
async fn a_spooled_page_without_columns_is_an_error() {
    let cli = ClientBuilder::new("u", "localhost")
        .port(8080)
        .build()
        .unwrap();
    let data: QueryResultData<Row> = serde_json::from_value(json!({
        "encoding": "json",
        "segments": [{"type": "inline", "data": "W1sxXV0=", "metadata": {"rowsCount": 1}}]
    }))
    .unwrap();

    let err = cli.decode_page(data, None).await.unwrap_err();

    assert!(
        err.to_string().contains("Column metadata"),
        "expected the missing-column-metadata error, got: {err}"
    );
}
