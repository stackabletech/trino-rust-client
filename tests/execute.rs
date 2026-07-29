//! `execute` must read its result from the page it already fetched — not
//! re-fetch it on an unauthenticated, unretried request.

use std::time::Duration;
use trino_rust_client::auth::Auth;
use trino_rust_client::client::ClientBuilder;
use trino_rust_client::retry::RetryPolicy;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

async fn server_addr(server: &MockServer) -> (String, u16) {
    let uri = server.uri();
    let host_port = uri.trim_start_matches("http://");
    let (host, port_str) = host_port.rsplit_once(':').unwrap();
    (host.to_string(), port_str.parse().unwrap())
}

/// A statement response. `next` is the path to follow, or `None` for the final
/// page. The final page carries the update fields, exactly as Trino does.
fn page(server_uri: &str, id: &str, next: Option<&str>, update: bool) -> String {
    let next_uri = match next {
        Some(p) => format!(r#""nextUri": "{server_uri}{p}","#),
        None => String::new(),
    };
    let update_fields = if update {
        r#""updateType": "INSERT", "updateCount": 7,"#
    } else {
        ""
    };
    format!(
        r#"{{
            "id": "{id}",
            "infoUri": "{server_uri}/ui/query.html?{id}",
            {next_uri}
            {update_fields}
            "stats": {{
                "state": "FINISHED", "queued": false, "scheduled": false,
                "nodes": 0, "totalSplits": 0, "queuedSplits": 0,
                "runningSplits": 0, "completedSplits": 0,
                "cpuTimeMillis": 0, "wallTimeMillis": 0, "queuedTimeMillis": 0,
                "elapsedTimeMillis": 0, "processedRows": 0, "processedBytes": 0,
                "peakMemoryBytes": 0, "spilledBytes": 0
            }},
            "warnings": []
        }}"#
    )
}

/// Every request `execute` makes must be authenticated. The final result read
/// used to be a bare `client.get`, which carried no Authorization header.
#[tokio::test]
async fn execute_authenticates_every_request() {
    let server = MockServer::start().await;
    let uri = server.uri();

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(
            &uri,
            "q1",
            Some("/v1/statement/q1/1"),
            false,
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/statement/q1/1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(&uri, "q1", None, true)))
        .mount(&server)
        .await;

    let (host, port) = server_addr(&server).await;
    let client = ClientBuilder::new("user", host)
        .port(port)
        .auth(Auth::new_basic("user", Some("pw")))
        .auth_http_insecure(true)
        .build()
        .unwrap();

    let res = client.execute("INSERT INTO t VALUES (1)").await.unwrap();
    assert_eq!(res.update_type.as_deref(), Some("INSERT"));
    assert_eq!(res.update_count, Some(7));

    let reqs: Vec<Request> = server.received_requests().await.unwrap();
    let unauthed: Vec<_> = reqs
        .iter()
        .filter(|r| r.headers.get("authorization").is_none())
        .map(|r| format!("{} {}", r.method, r.url.path()))
        .collect();
    assert!(
        unauthed.is_empty(),
        "these requests went out unauthenticated: {unauthed:?}"
    );

    // Two requests: the POST and one page fetch. The old code re-fetched the
    // terminal page a second time.
    assert_eq!(reqs.len(), 2, "execute must not re-fetch the terminal page");
}

/// `execute` used to re-fetch the terminal page on a bare, unretried request
/// after the paging loop had already read it. That made a transient failure on
/// a page the client had *already successfully read* fatal to the statement —
/// and to every transaction verb, which all route through `execute`.
///
/// The terminal page is served once and then 503s. Reading the result from the
/// page the loop already holds means the second request never happens.
#[tokio::test]
async fn execute_does_not_refetch_a_page_it_already_read() {
    let server = MockServer::start().await;
    let uri = server.uri();

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(
            &uri,
            "q1",
            Some("/v1/statement/q1/1"),
            false,
        )))
        .mount(&server)
        .await;
    // The terminal page, served exactly once.
    Mock::given(method("GET"))
        .and(path("/v1/statement/q1/1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(&uri, "q1", None, true)))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    // Any repeat fetch of it fails.
    Mock::given(method("GET"))
        .and(path("/v1/statement/q1/1"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let (host, port) = server_addr(&server).await;
    let client = ClientBuilder::new("user", host)
        .port(port)
        .retry_policy(RetryPolicy {
            max_retries: 3,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            jitter: false,
        })
        .build()
        .unwrap();

    let res = client
        .execute("INSERT INTO t VALUES (1)")
        .await
        .expect("execute must read the terminal page it already fetched");
    assert_eq!(res.update_count, Some(7));

    let gets = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/v1/statement/q1/1")
        .count();
    assert_eq!(gets, 1, "the terminal page must be fetched exactly once");
}

/// A statement whose POST response carries no nextUri reads its update fields
/// from that response, instead of failing with "No next URI available".
#[tokio::test]
async fn execute_handles_a_single_page_statement() {
    let server = MockServer::start().await;
    let uri = server.uri();

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(&uri, "q1", None, true)))
        .mount(&server)
        .await;

    let (host, port) = server_addr(&server).await;
    let client = ClientBuilder::new("user", host).port(port).build().unwrap();

    let res = client.execute("CREATE TABLE t (n int)").await.unwrap();
    assert_eq!(res.update_type.as_deref(), Some("INSERT"));
    assert_eq!(res.update_count, Some(7));
}
