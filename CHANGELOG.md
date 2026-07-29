# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://book.async.rs/overview/stability-guarantees.html).

## [Unreleased]

### Added
- `Client::pager` / `QueryPager` — page through a query one page at a time, with the schema (`columns()`) and `query_id()` resolved up front and an explicit `cancel()`. Resolves spooled segments correctly, so a consumer that needs incremental delivery *and* a cancellable query no longer has to hand-roll paging over `get`/`get_next` (which cannot decode spooled data)
- `RowStream::query_id` and `RowStream::cancel` — inspect the streaming query's id and cancel it explicitly, reporting failure. `cancel` goes through the client's normal request path, so it retries under the `RetryPolicy` and re-authenticates an expired OAuth2 token, and it treats `204` as success — matching `QueryPager::cancel`. Dropping a `RowStream` still cancels best-effort without reporting, using only a cached token: a destructor must not block on an interactive login
- Spooled segment fetches now honour the client's `RetryPolicy`. A transient object-storage failure (HTTP 502/503/504, connect/timeout) is retried per segment, like the coordinator page fetch beside it; terminal responses such as an expired presigned URL (403) still fail fast. Previously segment fetches never retried
- `Client::begin_transaction`, `Client::commit` and `Client::rollback` for driving Trino transactions, plus `Client::transaction_id` / `Client::set_transaction_id` to inspect and set the session's transaction at runtime (previously only settable at build time via `ClientBuilder::transaction_id`). `begin_transaction` verifies that the coordinator actually returned a transaction id, so `Ok(())` means a transaction is active — a `START TRANSACTION` that succeeds without a usable `X-Trino-Started-Transaction-Id` (a header-stripping proxy, or the `NONE` sentinel) is reported as `Error::Transaction` instead of silently leaving later statements outside any transaction
- `Error::Transaction` — returned when a transaction operation is attempted in a state that does not allow it (starting one while another is active, or committing/rolling back without one)
- `TransactionId::is_active`
- Interactive OAuth2 authentication (`Auth::new_oauth2` / `new_oauth2_with_handler`). On a `401` Bearer challenge the client presents the login URL (browser + stderr by default, or a custom `RedirectHandler`), polls the Trino token endpoint, and retries with the bearer token. Token is cached in-memory for the process. Coordinators with several authentication types enabled (e.g. `http-server.authentication.type=PASSWORD,OAUTH2`) send one `WWW-Authenticate` header per type, so every challenge header is scanned for the Bearer one rather than only the first
- `Auth::new_oauth2_with_config(OAuth2Config { .. })` — set the redirect handler and the token-poll budget when the `Auth` is built
- `ClientBuilder::timezone` and `ClientBuilder::role` / `ClientBuilder::roles` — `Session::timezone` and `Session::roles` were sent on the wire and parsed back from `X-Trino-Set-Role`, but nothing could construct them, so the coordinator's default time zone always won and the role map was permanently empty. Roles are required by Hive and Iceberg under `sql-standard` security. `chrono_tz::Tz` is re-exported as `trino_rust_client::Tz` so setting a time zone needs no extra dependency
- `Client::session_snapshot` — read the session state Trino changes at runtime. `USE`, `SET SESSION`, `SET ROLE` and `PREPARE` were all tracked from the `X-Trino-Set-*` response headers and none of it was readable, so an application could not tell which catalog or schema it was actually on after a `USE`. The snapshot is taken under one read lock, so its fields are mutually consistent
- `ClientBuilder::session_user` — run statements as another user (`X-Trino-User`) while authenticating as the connection principal, matching the JDBC driver's `sessionUser`. Builder-time only: session state accumulates per client, so one client per impersonated user keeps it from leaking across identities
- `ClientBuilder::locale` — sent as `X-Trino-Language`. The header constant existed but had no backing field
- `ClientBuilder::extra_header` / `extra_headers` — arbitrary headers on every request, for gateways and proxies that need them. Validated at `build` time: a malformed name or value is `Error::InvalidHeader`, and a collision with a header the client manages is `Error::ReservedHeader`, because `reqwest` appends rather than replaces and a collision would send two values instead of one
- `ClientBuilder::client_capability` / `client_capabilities` — advertise extra capabilities on top of `PARAMETRIC_DATETIME` and `PATH`, which the client always sends and cannot be told to drop (the type decoder depends on both)
- `ClientBuilder::proxy` and `Proxy` — route all traffic through an HTTP or HTTPS proxy, optionally with Basic credentials. SOCKS is rejected at construction rather than failing at connect time, since reqwest's `socks` feature is not enabled
- `Ssl::identity` and `Identity` — client-certificate (mutual TLS) authentication, from a PEM holding the certificate chain and a PKCS#8 key. PEM only: the crate builds reqwest on rustls, which accepts neither PKCS#12 nor JKS
- `TlsVerification` — `Full` (default), `CaOnly` (verify the chain, skip the hostname, matching JDBC's `SSLVerification=CA`) and `None`. `CaOnly` requires a root certificate set with `Ssl::root_cert`: rustls only permits skipping hostname verification when the trust store is supplied explicitly, and doing so excludes the platform's own roots — so without one the client would trust nothing at all. `build` reports that rather than failing every connection

### Fixed
- **Spooled results silently returned zero rows.** On the `Client::get` / `Client::get_next` paging path, `QueryResultData::into_vec` caught every spooled-segment failure and returned an empty `Vec`, so a consumer received no rows and no error for any result the coordinator spooled — data loss presented as success. Without the `spooling` feature the same input panicked instead. `into_vec` now returns `Result` and never panics. Spooled result data is now resolved on the manual paging path too, via the new `Client::pager`
- **Enabling the `spooling` cargo feature changed wire behaviour on its own.** The session defaulted to advertising `X-Trino-Query-Data-Encoding: json+zstd`, and there was no way to turn it off, so simply compiling the feature — including because another crate in the dependency graph enabled it — made the coordinator spool results. Spooling is now opt-in via `ClientBuilder::spooling_encoding`
- `Client::get_all` no longer fails with `Cannot mix Direct and Spooled protocols in same query` when a query's pages switch protocol; all paging now shares one implementation
- **Transactions were unusable.** Trino returns a new transaction's identifier in `X-Trino-Started-Transaction-Id`, but the client parsed that header with a function that recognised only four fixed literals. A real identifier matched none of them and was silently discarded, so `START TRANSACTION` succeeded on the coordinator while every subsequent statement sent `X-Trino-Transaction-Id: NONE` and ran outside the transaction — and `COMMIT`/`ROLLBACK` could not address it. The identifier is now retained and sent on every subsequent request
- **`execute` re-fetched its last page outside the retry policy.** After draining every page it discarded each response and re-requested the final `nextUri` on a bare HTTP call — no authentication, no session headers and no `RetryPolicy`, so a single transient 502/503 there failed the whole statement, and with it `begin_transaction` / `commit` / `rollback`, which all route through `execute`. The terminal page already carries `updateType` / `updateCount` / `error`, so that request is gone: `execute` now reads its result from the page it already fetched, halving the request count. A statement whose first response carries no `nextUri` no longer fails with `No next URI available for execution result`
- A failed spooled-segment fetch keeps its error class. `fetch_segments` wrapped every per-segment failure in `Error::InternalError`, so an expired presigned URL arrived as an opaque internal error instead of `Error::HttpNotOk(403, ..)` and could not be told apart from a bug. The segment index now goes to a `tracing::warn!` instead of into the error type
- Unparseable `X-Trino-Set-Role` header values are now logged instead of being dropped silently

### Changed
- **Breaking:** `QueryResultData::into_vec` returns `Result<Vec<T>, Error>` and no longer decodes spooled pages at all (inline segments included). Decoding spooled rows needs the HTTP client and the query's column metadata, which `QueryResultData` does not have; use `Client::pager`, `Client::stream` or `Client::get_all`. See the [migration guide](MIGRATION.md)
- **Breaking:** `ClientBuilder::spooling_encoding` takes a typed `SpoolingEncoding` instead of `impl ToString`, and spooling is opt-in (previously it defaulted to `json+zstd`). An invalid encoding string is no longer silently replaced by `json+zstd` with a warning — convert explicitly with `SpoolingEncoding::try_from(..)`. `Session::spooling_encoding` is now `Option<SpoolingEncoding>` and only exists under the `spooling` feature. See the [migration guide](MIGRATION.md)
- **Breaking:** `TransactionId` now models what the `X-Trino-Transaction-Id` header actually carries: `NoTransaction | Id(String)`. The `StartTransaction`, `RollBack` and `Commit` variants are removed — they are SQL statements, not header values, and sending them produced a header Trino does not accept. `to_str` is replaced by `as_header_value(&self) -> &str` and `from_str` by the infallible `from_header_value(&str) -> Self`. `TransactionId` is no longer `Copy` (it now owns a `String`); it is still `Clone`, and now also `PartialEq` and `Eq`. See the [migration guide](MIGRATION.md)
- **Breaking:** `Auth` is now `#[non_exhaustive]` and has a new `OAuth2` variant. Exhaustive `match` on `Auth` must add a wildcard arm
- **Breaking:** OAuth2 token polling is bounded by a single deadline. `max_poll_attempts` is gone and `poll_timeout` is the only budget, raised from 120s to **300s** — Trino long-polls the token endpoint for ~10s, so the old 10-attempt cap ended the login after ~100s and the 120s timeout never applied. A `min_poll_interval` (default 1s) now paces the loop so a coordinator or proxy that answers immediately cannot exhaust the budget in milliseconds. See the [migration guide](MIGRATION.md)
- **Breaking:** `error::TrinoRetryResult` and `error::TrinoStats` are removed. They existed only for the redundant final fetch in `execute`; `QueryResult` supersedes them with the same fields and a fully typed `stats`. See the [migration guide](MIGRATION.md)
- **Breaking:** `Auth::with_poll` is removed; pass an `OAuth2Config` to `Auth::new_oauth2_with_config` instead. `with_poll` rebuilt the shared `OAuth2State`, so calling it discarded any cached token and left existing `Client` clones on the old settings. See the [migration guide](MIGRATION.md)
- **Breaking:** `ClientBuilder::no_verify(bool)` is replaced by `ClientBuilder::tls_verification(TlsVerification)`. Two verification knobs with a precedence rule between them is how connections end up silently insecure, so the boolean is gone rather than deprecated. See the [migration guide](MIGRATION.md)
- **Breaking:** `Ssl`'s fields are private; construct it with `Ssl::new().root_cert(..).identity(..)`. This is what makes room for the client-certificate identity without a second breaking change later. See the [migration guide](MIGRATION.md)
- `X-Trino-Client-Capabilities` is now sorted, so the default value is `PARAMETRIC_DATETIME,PATH` rather than `PATH,PARAMETRIC_DATETIME`. Trino treats it as a set, so this is not a behavioural change
- `src/client.rs` is split into `src/client/mod.rs` and `src/client/builder.rs`. No public path changes

## [0.11.0] - 2026-07-19

> Upgrading from 0.10.x? See the [migration guide](MIGRATION.md).

### Added
- `Client::get_all`, `Client::get` and `Client::execute` now accept `impl Into<String>` for the SQL, so `&str` literals work without `.to_string()` (consistent with `Client::stream`). Existing `String` call sites are unaffected
- `VarBinary` type for Trino `VARBINARY` columns — decodes/encodes the base64 wire format into raw bytes (`base64` is now a core dependency). Confirmed that `Json` (`serde_json::Value`) and `TimestampWithTimeZone` (`chrono::DateTime<FixedOffset>`) already decode, and added tests
- Crate-level documentation with a runnable quickstart, and doc comments on the core public API (`Client`, `ClientBuilder`, `get_all`/`get`/`get_next`/`cancel`, `ExecuteResult`). docs.rs now builds with all features
- Configurable retry/backoff via `RetryPolicy` (`max_retries`, `min_delay`, `max_delay`, `jitter`) and `ClientBuilder::retry_policy`. The default preserves the previous behaviour (3 retries, 1s→2s, no jitter). Retries are now **idempotency-aware**: idempotent page fetches (`GET nextUri`) retry on any transient failure (HTTP 502/503/504, connect/timeout), while query submission (`POST /v1/statement`) only retries when the request was definitely not processed (503, connection failures) so a non-idempotent statement (`INSERT`/DDL via `execute`) is never submitted twice. Query, decode, protocol and other errors fail fast
- `QueryError::kind()` returning a `#[non_exhaustive]` `TrinoErrorKind` enum, for ergonomic, discoverable matching on the common Trino error names (`TableNotFound`, `SchemaNotFound`, `SyntaxError`, …) without comparing raw strings; falls back to `TrinoErrorKind::Other`
- `Client::stream` — lazily stream query rows page by page without buffering the whole result set in memory (Direct and Spooled protocols). Returns a `RowStream` that resolves the result columns up front (`RowStream::columns() -> &[Column]`), implements `futures::Stream`, is `Send`/`Unpin`, and best-effort cancels the query on the coordinator if dropped before completion

### Changed
- Declared a minimum supported Rust version (MSRV) of **1.86** and added a CI job that enforces it
- Unsupported Trino column types now fail with `Error::UnsupportedType` naming the type (e.g. `unsupported Trino type: HyperLogLog`) instead of a generic error
- Switched logging from the `log` crate to [`tracing`](https://docs.rs/tracing): the same events are now emitted via `tracing`, and `get_all` / `stream` / `execute` are wrapped in a span carrying the `query_id` for per-query correlation. Install a `tracing` subscriber to see them
- The library now depends on `tokio` with only the `rt` and `sync` features instead of `full`, shrinking the dependency footprint and compile time for consumers (no longer pulls `fs`/`process`/`signal`/…). Tests and examples keep the fuller feature set via dev-dependencies
- **Breaking:** Restructured the `Error` enum for consistent, matchable errors. A Trino query failure is now a single `Error::Query(Box<QueryError>)` carrying the full structured error — match on `error_code` / `error_name` / `error_type`, or reach it via `std::error::Error::source()` (the top-level `Display` stays concise: `query error [NAME]: message`). Both the query and execute code paths now map failures identically (and `error_code == 4` still maps to `Error::Forbidden`). Added typed `Error::Decode`, `Error::Tls` and `Error::Protocol` variants in place of many stringly `InternalError`s. `Error` is now `#[non_exhaustive]` so future variants can be added without a breaking change
- **Breaking:** Unified the two Trino error representations — removed `error::TrinoError`/`TrinoErrorLocation`; `TrinoRetryResult::error` is now `Option<models::QueryError>`, and `QueryError::failure_info` is now `Option<FailureInfo>`

### Removed
- **Breaking:** Removed the name-mapped `Error` variants (`CatalogNotFound`, `SchemaNotFound`, `TableNotFound`, `TableAlreadyExists`, `InvalidCatalog`, …) and the unused `Error::EmptyData` (no longer produced since zero-row queries return an empty result). Match on the structured `Error::Query`'s `error_name` instead

## [0.10.0] - 2026-07-18
### Security
- Updated dependencies to remediate 13 RustSec advisories in transitive crates, including `aws-lc-sys` (X.509/PKCS7 validation bypasses, timing side-channel), `quinn-proto` (DoS, memory exhaustion), `rustls-webpki` (CRL/name-constraint validation, parsing panic), `bytes` (integer overflow) and `slab` (out-of-bounds, yanked) [#44](https://github.com/nudibranches-tech/trino-rust-client/pull/44)

### Changed
- Refreshed direct dependency versions (`backon`, `chrono`, `futures`, `http`, `log`, `regex`, `reqwest`, `serde_json`, `tokio`, `tracing-subscriber`, `uuid`) [#44](https://github.com/nudibranches-tech/trino-rust-client/pull/44)
- Replaced the unmaintained `dotenv` dev-dependency with the maintained `dotenvy` [#44](https://github.com/nudibranches-tech/trino-rust-client/pull/44)
- Bumped `trino-rust-client-macros` to 0.7.2 (adds `#[trino(rename = "...")]` support)

### Removed
- **Breaking:** Removed the unused `Trino` feature [#40](https://github.com/nudibranches-tech/trino-rust-client/pull/40)
- **Breaking:** The optional `spooling` codec dependencies (`base64`, `zstd`, `lz4`, `flate2`) are no longer exposed as standalone public features; they are now declared via `dep:` and can only be enabled through the `spooling` feature [#40](https://github.com/nudibranches-tech/trino-rust-client/pull/40)

### Added
- Support configuring the name of the result field when using the `Trino` derive macro [#43](https://github.com/nudibranches-tech/trino-rust-client/pull/43)
- Derive `Serialize` on the result, error, stat, warning and segment model types to allow serializing query results (credit to [@sbernauer](https://github.com/sbernauer), originally [#42](https://github.com/nudibranches-tech/trino-rust-client/pull/42))

## [0.9.3] - 2026-02-19
### Added
- `auth_http_insecure` option to allow authentication over HTTP [#34](https://github.com/nudibranches-tech/trino-rust-client/pull/34)

### Changed
- Boxed `reqwest::Error` and `QueryError` in error enum to reduce `Result` size [#32](https://github.com/nudibranches-tech/trino-rust-client/pull/32)
- Removed unnecessary `clippy::result_large_err` suppressions [#32](https://github.com/nudibranches-tech/trino-rust-client/pull/32)
- Masked JWT token in `Auth::Jwt` debug output [#34](https://github.com/nudibranches-tech/trino-rust-client/pull/34)
- Refreshed dependency versions [#35](https://github.com/nudibranches-tech/trino-rust-client/pull/35)

## [0.9.2] - 2025-11-20
### Changed
- Bumped trino-rust-client-macros to 0.7.1
- Integration tests are no longer published to crates.io

## [0.9.1] - 2025-11-20
### Added
- Spooling protocol support for efficient large result set handling [#26](https://github.com/nudibranches-tech/trino-rust-client/pull/26)
- New `spooling_encoding` configuration option (supports json+zstd, json+lz4, json+gzip)
- New `max_concurrent_segments` configuration for controlling concurrent downloads
- Spooling feature flag with compression support (zstd, lz4, gzip)

### Changed
- Bumped trino-rust-client-macros to 0.7.0

## [0.8.0] - 2025-01-XX
### Added
- Return execute result with operation type and counts [#24](https://github.com/nudibranches-tech/trino-rust-client/pull/24)

### Fixed
- Fixed prepared statement header decoding [#23](https://github.com/nudibranches-tech/trino-rust-client/pull/23)
- Ensure finished prepared statement's results are parsed accordingly [#22](https://github.com/nudibranches-tech/trino-rust-client/pull/22)

## [0.5.1] - 2023-10-19
- Make Client::get and some functions public [#29](https://github.com/nooberfsh/prusto/pull/29)

## [0.5.0] - 2023-02-27
- v0.5.0 can be used with stable rust.
- Add SSL root certificate support [#22](https://github.com/nooberfsh/prusto/pull/22)
- Provide a feature flag for running as presto client [#19](https://github.com/nooberfsh/prusto/pull/19)

## [0.4.0] - 2022-02-07
- Use `Rust 2021`

## [0.3.0] - 2021-05-26
- Use `Trino` protocol
- Add `execute` to `Client`
- Add more session properties
- Fix deserialization of `ClientTypeSignatureParameter`

## [0.2.0] - 2021-01-06
- Add `len`, `as_slice` methods to `DataSet<T>`
- Update `tokio` stack to 1.0
- Use `rustls` instead of `native-tls`

## [0.1.2] - 2020-10-30
-  Make `QueryError::error_location` optional

## [0.1.1] - 2020-10-09
- Add `'static` bound to key and value types of map like types

## [0.1.0] - 2020-10-01
- Initial release

[Unreleased]: https://github.com/nudibranches-tech/trino-rust-client/compare/0.11.0...HEAD
[0.11.0]: https://github.com/nudibranches-tech/trino-rust-client/compare/0.10.0...0.11.0
[0.10.0]: https://github.com/nudibranches-tech/trino-rust-client/compare/0.9.3...0.10.0
[0.9.3]: https://github.com/nudibranches-tech/trino-rust-client/compare/0.9.2...0.9.3
[0.9.2]: https://github.com/nudibranches-tech/trino-rust-client/compare/0.9.1...0.9.2
[0.9.1]: https://github.com/nudibranches-tech/trino-rust-client/compare/0.8.0...0.9.1
[0.8.0]: https://github.com/nudibranches-tech/trino-rust-client/compare/0.5.1...0.8.0
[0.5.1]: https://github.com/nooberfsh/prusto/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/nooberfsh/prusto/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/nooberfsh/prusto/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/nooberfsh/prusto/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nooberfsh/prusto/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/nooberfsh/prusto/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/nooberfsh/prusto/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/nooberfsh/prusto/tree/v0.1.0
