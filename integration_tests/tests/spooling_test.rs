use futures::FutureExt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use trino_integration_tests::{set_test_fixture, TestFixture};
use trino_rust_client::spooling::{SegmentFetcher, SpoolingEncoding};
use trino_rust_client::{Client, ClientBuilder, Row, Trino};
use uuid::Uuid;

/// Helper to create a client that can resolve 'minio' hostname to localhost
/// This is needed because Trino running in Docker returns 'http://minio:9000/...'
/// but the test running on host needs to access it via localhost:9000.
fn create_test_client(fixture: &TestFixture, encoding: SpoolingEncoding) -> Client {
    let docker_port = 9003;
    let http_client = reqwest::Client::builder()
        .resolve(
            "minio",
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), docker_port),
        )
        .build()
        .expect("Failed to create http client");

    let fetcher = SegmentFetcher::new(http_client);
    // Enable spooling protocol
    ClientBuilder::new("test", &fixture.coordinator_host)
        .port(fixture.coordinator_port)
        .catalog("memory")
        .schema("default")
        .spooling_encoding(encoding)
        .max_concurrent_segments(5)
        .segment_fetcher(fetcher)
        .build()
        .expect("Failed to create client")
}

/// Helper to run a test with a temporary table that gets cleaned up automatically.
async fn with_temp_table<F, Fut>(client: &Client, create_sql_template: &str, test_fn: F)
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let table_name = format!("memory.default.table_{}", Uuid::new_v4().simple());
    let create_sql = create_sql_template.replace("{}", &table_name);
    client
        .execute(create_sql)
        .await
        .expect("Failed to create temp table");

    let result = std::panic::AssertUnwindSafe(test_fn(table_name.clone()))
        .catch_unwind()
        .await;

    let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
    if let Err(e) = client.execute(drop_sql).await {
        eprintln!("Failed to drop temp table {}: {:?}", table_name, e);
    }

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[tokio::test]
async fn test_spooling_protocol_encodings() {
    let fixture = set_test_fixture("test_spooling_protocol_encodings");

    // Client for setup (standard client is fine for setup)
    let setup_client = ClientBuilder::new("test", &fixture.coordinator_host)
        .port(fixture.coordinator_port)
        .catalog("memory")
        .schema("default")
        .build()
        .expect("Failed to create setup client");

    // Create table with 100 rows (enough to trigger spooling limit of 10)
    let create_sql_template = r#"
        CREATE TABLE {} AS
        SELECT 
            CAST(n AS BIGINT) AS id,
            CONCAT('user_', CAST(n AS VARCHAR)) AS name,
            CAST(n * 10 AS BIGINT) AS value
        FROM UNNEST(SEQUENCE(1, 100)) AS t(n)
    "#;

    with_temp_table(
        &setup_client,
        create_sql_template,
        |table_name| async move {
            let encodings = vec![
                SpoolingEncoding::Json,
                SpoolingEncoding::JsonZstd,
                SpoolingEncoding::JsonLz4,
            ];

            for encoding in encodings {
                // Create client with custom DNS resolution and specific encoding
                let client = create_test_client(&fixture, encoding);

                let select_sql = format!("SELECT * FROM {} ORDER BY id", table_name);
                let rows = client
                    .get_all::<Row>(select_sql)
                    .await
                    .unwrap_or_else(|e| panic!("Query with {} encoding failed: {:?}", encoding, e));

                assert_eq!(
                    rows.len(),
                    100,
                    "{} encoding should return all 100 rows",
                    encoding
                );

                let first = &rows.as_slice()[0];
                assert_eq!(
                    first.value().first().unwrap(),
                    &1i64,
                    "First ID mismatch ({})",
                    encoding
                );
                assert_eq!(
                    first.value().get(1).unwrap(),
                    &"user_1".to_string(),
                    "First Name mismatch ({})",
                    encoding
                );

                let last = &rows.as_slice()[99];
                assert_eq!(
                    last.value().first().unwrap(),
                    &100i64,
                    "Last ID mismatch ({})",
                    encoding
                );
                assert_eq!(
                    last.value().get(1).unwrap(),
                    &"user_100".to_string(),
                    "Last Name mismatch ({})",
                    encoding
                );
            }
        },
    )
    .await;
}

/// The regression this branch fixes: paging manually over a spooled result.
///
/// Before the fix, `Client::get`/`get_next` + `QueryResultData::into_vec`
/// returned **zero rows and no error** for anything the coordinator spooled.
/// The coordinator here is configured to spool aggressively
/// (`protocol.spooling.inlining.max-rows=1`), so every page is spooled.
#[tokio::test]
async fn test_pager_reads_spooled_rows_incrementally() {
    let fixture = set_test_fixture("test_pager_reads_spooled_rows_incrementally");

    let setup_client = ClientBuilder::new("test", &fixture.coordinator_host)
        .port(fixture.coordinator_port)
        .catalog("memory")
        .schema("default")
        .build()
        .expect("Failed to create setup client");

    let create_sql_template = r#"
        CREATE TABLE {} AS
        SELECT
            CAST(n AS BIGINT) AS id,
            CONCAT('user_', CAST(n AS VARCHAR)) AS name
        FROM UNNEST(SEQUENCE(1, 1000)) AS t(n)
    "#;

    with_temp_table(
        &setup_client,
        create_sql_template,
        |table_name| async move {
            let client = create_test_client(&fixture, SpoolingEncoding::JsonZstd);
            let select_sql = format!("SELECT * FROM {} ORDER BY id", table_name);

            let mut pager = client
                .pager::<Row>(select_sql)
                .await
                .expect("pager should submit the query and resolve the schema");

            // The schema and query id are available before any row arrives — this is
            // what a driver needs to stay cancellable while streaming.
            assert!(
                !pager.query_id().is_empty(),
                "query id must be exposed for cancellation"
            );
            assert_eq!(pager.columns().len(), 2, "schema resolved up front");

            let mut total = 0usize;
            let mut pages = 0usize;
            let mut first_id: Option<i64> = None;
            let mut last_id: Option<i64> = None;

            while let Some(rows) = pager.next_page().await.expect("page must decode") {
                pages += 1;
                for row in &rows {
                    let id = row.value().first().unwrap().as_i64().unwrap();
                    if first_id.is_none() {
                        first_id = Some(id);
                    }
                    last_id = Some(id);
                }
                total += rows.len();
            }

            assert_eq!(total, 1000, "every spooled row must be delivered");
            assert!(pages > 0, "rows must arrive incrementally, page by page");
            assert_eq!(first_id, Some(1), "first row mismatch");
            assert_eq!(last_id, Some(1000), "last row mismatch");
        },
    )
    .await;
}
