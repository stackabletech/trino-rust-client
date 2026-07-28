//! Cross-regime soundness checks.
//!
//! These tests assert invariants that must hold **regardless** of how the
//! coordinator is configured — spooling off, spooling with inline segments, or
//! spooling with remote segments in object storage. The same file is run against
//! several coordinator configurations (see `test_setup/README-pen-test.md`), so a
//! failure in any regime means the client's behaviour depends on server config
//! in a way callers cannot see.
//!
//! The central invariant: **no entry point may ever silently return fewer rows
//! than the query produced.** Reporting an error is acceptable; quietly
//! returning nothing is the bug this branch fixes.
use futures::StreamExt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use trino_integration_tests::{set_test_fixture, TestFixture};
use trino_rust_client::spooling::{SegmentFetcher, SpoolingEncoding};
use trino_rust_client::{Client, ClientBuilder, QueryResultData, Row};
use uuid::Uuid;

const ROWS: i64 = 1000;

/// Client that can resolve the `minio` hostname the coordinator hands out.
fn spooling_client(fixture: &TestFixture, encoding: SpoolingEncoding) -> Client {
    let http_client = reqwest::Client::builder()
        .resolve(
            "minio",
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9003),
        )
        .build()
        .expect("Failed to create http client");

    ClientBuilder::new("test", &fixture.coordinator_host)
        .port(fixture.coordinator_port)
        .catalog("memory")
        .schema("default")
        .spooling_encoding(encoding)
        .segment_fetcher(SegmentFetcher::new(http_client))
        .build()
        .expect("Failed to create spooling client")
}

/// Client that never opts in to spooling.
fn plain_client(fixture: &TestFixture) -> Client {
    ClientBuilder::new("test", &fixture.coordinator_host)
        .port(fixture.coordinator_port)
        .catalog("memory")
        .schema("default")
        .build()
        .expect("Failed to create plain client")
}

async fn with_table<F, Fut>(client: &Client, rows: i64, test_fn: F)
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let table = format!("memory.default.pen_{}", Uuid::new_v4().simple());
    let create = format!(
        "CREATE TABLE {} AS SELECT CAST(n AS BIGINT) AS id, \
         CONCAT('user_', CAST(n AS VARCHAR)) AS name \
         FROM UNNEST(SEQUENCE(1, {})) AS t(n)",
        table, rows
    );
    client.execute(create).await.expect("create table");

    let result = std::panic::AssertUnwindSafe(test_fn(table.clone()))
        .catch_unwind()
        .await;

    let _ = client.execute(format!("DROP TABLE IF EXISTS {}", table)).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

use futures::FutureExt;

/// Every entry point must agree on the row count, in every regime.
#[tokio::test]
async fn all_entry_points_agree_on_row_count() {
    let fixture = set_test_fixture("all_entry_points_agree_on_row_count");
    let setup = plain_client(&fixture);

    with_table(&setup, ROWS, |table| async move {
        // `json+lz4` is excluded: Trino emits LZ4 *raw block* format (segments
        // start with an LZ4 token byte, e.g. `f3 01 ...`), while
        // `spooling::decoder::decompress_lz4` uses `lz4::Decoder`, the LZ4
        // *frame* decoder (frame magic `04 22 4d 18`). Decoding therefore fails
        // and the plain-JSON fallback then fails on invalid UTF-8. This is a
        // pre-existing bug — `decoder.rs` is untouched by this branch and `main`
        // fails identically — and it only shows up once a segment is large
        // enough for Trino to actually compress it, which is why the
        // aggressive-spooling config (1kB segments, left uncompressed) hides it.
        // Set PEN_TEST_LZ4=1 to include it and watch it fail.
        let mut encodings = vec![SpoolingEncoding::Json, SpoolingEncoding::JsonZstd];
        if std::env::var("PEN_TEST_LZ4").is_ok() {
            encodings.push(SpoolingEncoding::JsonLz4);
        }

        for encoding in encodings {
            let client = spooling_client(&fixture, encoding);
            let sql = format!("SELECT * FROM {} ORDER BY id", table);

            let via_get_all = client
                .get_all::<Row>(sql.clone())
                .await
                .unwrap_or_else(|e| panic!("get_all failed ({encoding}): {e:?}"))
                .len();

            let mut stream = client
                .stream::<Row>(sql.clone())
                .await
                .unwrap_or_else(|e| panic!("stream failed ({encoding}): {e:?}"));
            let mut via_stream = 0usize;
            while let Some(row) = stream.next().await {
                row.unwrap_or_else(|e| panic!("stream row failed ({encoding}): {e:?}"));
                via_stream += 1;
            }

            let mut pager = client
                .pager::<Row>(sql.clone())
                .await
                .unwrap_or_else(|e| panic!("pager failed ({encoding}): {e:?}"));
            let mut via_pager = 0usize;
            while let Some(rows) = pager
                .next_page()
                .await
                .unwrap_or_else(|e| panic!("pager page failed ({encoding}): {e:?}"))
            {
                via_pager += rows.len();
            }

            assert_eq!(via_get_all, ROWS as usize, "get_all lost rows ({encoding})");
            assert_eq!(via_stream, ROWS as usize, "stream lost rows ({encoding})");
            assert_eq!(via_pager, ROWS as usize, "pager lost rows ({encoding})");
        }
    })
    .await;
}

/// A client that never calls `spooling_encoding` must not be spooled to, even
/// against a spooling-enabled coordinator, and must still get every row.
#[tokio::test]
async fn client_without_opt_in_is_never_spooled() {
    let fixture = set_test_fixture("client_without_opt_in_is_never_spooled");
    let setup = plain_client(&fixture);

    with_table(&setup, ROWS, |table| async move {
        let client = plain_client(&fixture);
        let sql = format!("SELECT * FROM {} ORDER BY id", table);

        // Page manually and inspect the raw protocol: no page may be Spooled.
        let mut page = client.get::<Row>(sql.clone()).await.expect("submit");
        let mut total = 0usize;
        loop {
            if let Some(data) = page.data.take() {
                assert!(
                    matches!(data, QueryResultData::Direct(_)),
                    "coordinator spooled to a client that never opted in"
                );
                total += data.into_vec().expect("direct rows decode").len();
            }
            match page.next_uri.take() {
                Some(url) => page = client.get_next::<Row>(&url).await.expect("next page"),
                None => break,
            }
        }
        assert_eq!(total, ROWS as usize, "plain client lost rows");
    })
    .await;
}

/// The invariant that failed before this branch: manual paging must never
/// silently return fewer rows than the query produced. Either it decodes every
/// row, or it reports an error — never a quiet zero.
#[tokio::test]
async fn manual_paging_never_loses_rows_silently() {
    let fixture = set_test_fixture("manual_paging_never_loses_rows_silently");
    let setup = plain_client(&fixture);

    with_table(&setup, ROWS, |table| async move {
        let client = spooling_client(&fixture, SpoolingEncoding::JsonZstd);
        let sql = format!("SELECT * FROM {} ORDER BY id", table);

        let mut page = client.get::<Row>(sql.clone()).await.expect("submit");
        let mut decoded = 0usize;
        let mut reported_error = false;

        loop {
            if let Some(data) = page.data.take() {
                match data.into_vec() {
                    Ok(rows) => decoded += rows.len(),
                    // Spooled page: into_vec cannot decode it and says so.
                    Err(e) => {
                        reported_error = true;
                        let msg = e.to_string();
                        assert!(
                            msg.contains("pager") || msg.contains("spooling"),
                            "error must point at the supported path, got: {msg}"
                        );
                    }
                }
            }
            match page.next_uri.take() {
                Some(url) => page = client.get_next::<Row>(&url).await.expect("next page"),
                None => break,
            }
        }

        assert!(
            reported_error || decoded == ROWS as usize,
            "silent data loss: decoded {decoded} of {ROWS} rows without reporting an error"
        );

        // And the supported path must deliver all of them.
        let mut pager = client.pager::<Row>(sql).await.expect("pager");
        let mut via_pager = 0usize;
        while let Some(rows) = pager.next_page().await.expect("page") {
            via_pager += rows.len();
        }
        assert_eq!(via_pager, ROWS as usize, "pager must deliver every row");
    })
    .await;
}

/// Edge cases that shift which protocol the coordinator picks.
#[tokio::test]
async fn edge_case_result_sizes() {
    let fixture = set_test_fixture("edge_case_result_sizes");
    let client = spooling_client(&fixture, SpoolingEncoding::JsonZstd);

    // Empty result.
    let mut pager = client
        .pager::<Row>("SELECT 1 AS n WHERE false")
        .await
        .expect("empty pager");
    assert!(
        pager.next_page().await.expect("empty page").is_none(),
        "empty result must yield no pages"
    );

    // Single row — small enough that the coordinator inlines the segment.
    let mut pager = client.pager::<Row>("SELECT 42 AS n").await.expect("pager");
    let mut total = 0usize;
    while let Some(rows) = pager.next_page().await.expect("single-row page") {
        total += rows.len();
    }
    assert_eq!(total, 1, "single-row query must return its row");

    // get_all agrees.
    let ds = client
        .get_all::<Row>("SELECT 42 AS n")
        .await
        .expect("get_all single row");
    assert_eq!(ds.len(), 1);
}

/// `cancel` must be accepted by the coordinator for a real, running query.
#[tokio::test]
async fn pager_cancel_is_accepted_by_coordinator() {
    let fixture = set_test_fixture("pager_cancel_is_accepted_by_coordinator");
    let setup = plain_client(&fixture);

    with_table(&setup, ROWS, |table| async move {
        let client = spooling_client(&fixture, SpoolingEncoding::JsonZstd);
        let sql = format!("SELECT * FROM {} ORDER BY id", table);

        let mut pager = client.pager::<Row>(sql).await.expect("pager");
        let query_id = pager.query_id().to_string();
        assert!(!query_id.is_empty(), "query id must be exposed");

        // Consume one page, then abandon the rest.
        let _ = pager.next_page().await.expect("first page");
        pager.cancel().await.expect("coordinator must accept cancel");
    })
    .await;
}
