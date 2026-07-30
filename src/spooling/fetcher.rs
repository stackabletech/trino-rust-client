use std::collections::HashMap;
use std::io::Read;
use std::thread;

use crate::client::need_retry_fetch;
use crate::error::{Error, Result};
use crate::retry::RetryPolicy;
use crate::spooling::segment::Segment;
use crate::spooling::segment::Segment::Inlined;
use backon::Retryable;
use base64::{engine::general_purpose, Engine as _};
use flate2::read::GzDecoder;
use futures::stream::{self, StreamExt};
use reqwest::Client;

/// Strip the query string from a segment URI before it reaches a log line or an
/// error message.
///
/// Spooled segment URIs are presigned: the query carries `X-Amz-Signature` (or
/// the equivalent for another object store), which is a bearer credential for
/// that object until it expires. Anyone who can read the log can fetch the
/// segment. The path alone still identifies which segment failed.
fn redact_uri(uri: &str) -> String {
    match uri.split_once('?') {
        Some((base, _)) => format!("{base}?<redacted>"),
        None => uri.to_string(),
    }
}

/// `reqwest` embeds the request URL in its `Display`, so a failed segment fetch
/// would print the presigned query string. Swap it for the redacted form —
/// dropping the URL entirely if it will not parse — while keeping the error
/// class intact, which the retry predicate depends on.
fn redact_reqwest_url(e: reqwest::Error) -> Error {
    match e.url().cloned() {
        Some(mut url) => {
            url.set_query(Some("redacted"));
            Error::from(e.with_url(url))
        }
        None => Error::from(e),
    }
}

// Default maximum number of concurrent segment fetches based on CPU count
fn default_max_concurrent_segments() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(5)
        .max(1)
}

/// One fetched segment: its bytes as they arrived, still in the page's encoding,
/// with the size they decompress to.
///
/// The two travel together because the `json+lz4` decoder needs the size and the
/// decoding happens after the fetch, by which point the [`Segment`] and its
/// metadata are gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentBytes {
    /// The segment's bytes, base64-decoded for an inline segment and downloaded
    /// for a spooled one, in both cases still compressed if the page's encoding
    /// says so.
    pub data: Vec<u8>,
    /// `uncompressedSize` from the segment metadata, when the coordinator sent
    /// it. Trino sends it for every segment under a compressed encoding.
    pub uncompressed_size: Option<usize>,
}

// Fetcher for segments with the spooling protocol
pub struct SegmentFetcher {
    http_client: Client,
    max_concurrent_segments: usize,
    retry: RetryPolicy,
}

// Fetcher for segments
impl SegmentFetcher {
    pub fn new(http_client: Client) -> Self {
        Self {
            http_client,
            max_concurrent_segments: default_max_concurrent_segments(),
            retry: RetryPolicy::default(),
        }
    }

    /// Adopt the owning client's retry policy. One policy governs both the
    /// coordinator page fetch and the segment fetches behind it.
    pub(crate) fn set_retry_policy(&mut self, policy: RetryPolicy) {
        self.retry = policy;
    }

    /// Configure the maximum number of concurrent segment fetches
    /// Default is based on available CPU parallelism (minimum 1)
    pub fn with_max_concurrent(mut self, count: usize) -> Self {
        self.max_concurrent_segments = count.max(1);
        self
    }

    /// Fetch a single segment and return the decoded data
    pub async fn fetch_segment(&self, segment: &Segment) -> Result<SegmentBytes> {
        let uncompressed_size = match segment {
            Inlined { metadata, .. } | Segment::Spooled { metadata, .. } => metadata
                .uncompressed_size()
                .and_then(|size| usize::try_from(size).ok()),
        };
        let data = match segment {
            Inlined { data, .. } => self.fetch_inline_segment(data).await?,
            Segment::Spooled {
                uri,
                ack_uri,
                headers,
                ..
            } => {
                // Retry per segment, not per batch: a batch-level retry would
                // re-fetch and re-ack every sibling segment on one failure.
                let fetch = || async { self.fetch_spooled_segment(uri, headers.as_ref()).await };
                let data = fetch
                    .retry(self.retry.backoff())
                    .when(need_retry_fetch)
                    .await?;

                // Acknowledgement is best-effort and deliberately not retried.
                if let Some(ack) = ack_uri {
                    if let Err(e) = self.acknowledge_segment(ack, headers.as_ref()).await {
                        tracing::warn!("Failed to acknowledge segment {}: {}", redact_uri(ack), e);
                    }
                }

                data
            }
        };

        Ok(SegmentBytes {
            data,
            uncompressed_size,
        })
    }

    /// Fetch multiple segments with controlled concurrency
    /// Uses buffered concurrency to limit the number of concurrent fetches
    pub async fn fetch_segments(&self, segments: Vec<Segment>) -> Result<Vec<SegmentBytes>> {
        tracing::debug!(
            "Fetching {} segments with max concurrency of {}",
            segments.len(),
            self.max_concurrent_segments
        );

        let results: Vec<Result<SegmentBytes>> = stream::iter(segments.into_iter().enumerate())
            .map(|(idx, segment)| async move {
                // Keep the error class intact — callers match on
                // `HttpNotOk`/`HttpError` to tell an expired presigned URL from
                // a transient outage. The segment index goes to the log rather
                // than into the error type.
                self.fetch_segment(&segment).await.inspect_err(|e| {
                    let uri = match &segment {
                        Inlined { .. } => "inline".to_string(),
                        Segment::Spooled { uri, .. } => uri.clone(),
                    };
                    tracing::warn!(
                        "Failed to fetch segment #{} ({}): {}",
                        idx,
                        redact_uri(&uri),
                        e
                    );
                })
            })
            .buffered(self.max_concurrent_segments)
            .collect()
            .await;

        // Collect results, returning early on first error
        results.into_iter().collect()
    }

    // Fetch an inline segment
    async fn fetch_inline_segment(&self, data: &str) -> Result<Vec<u8>> {
        general_purpose::STANDARD
            .decode(data)
            .map_err(|e| Error::Decode(format!("Base64 decode failed: {}", e)))
    }

    // Fetch a spooled segment (official Trino format)
    // Supports optional ackUri and headers fields
    async fn fetch_spooled_segment(
        &self,
        uri: &str,
        headers: Option<&HashMap<String, Vec<String>>>,
    ) -> Result<Vec<u8>> {
        tracing::debug!("Fetching spooled segment from: {}", redact_uri(uri));

        // Build GET request with optional headers
        let mut request = self.http_client.get(uri);

        // Apply headers if provided
        if let Some(headers_map) = headers {
            for (key, values) in headers_map {
                for value in values {
                    request = request.header(key, value);
                }
            }
        }

        // Execute GET request to the signed URI
        // NOTE: For local Docker testing, the client must be able to resolve
        // the storage hostname (e.g., 'minio') used in the URI. In production,
        // clients and Trino should share the same network view of storage endpoints.
        // Keep the reqwest error class intact: the retry predicate matches on
        // `Error::HttpError` / `Error::HttpNotOk`, so wrapping this in a
        // stringly `InternalError` would silently make every failure terminal.
        let response = request.send().await.map_err(redact_reqwest_url)?;

        // Check status
        if !response.status().is_success() {
            return Err(Error::HttpNotOk(
                response.status(),
                format!("Failed to fetch segment from {}", redact_uri(uri)),
            ));
        }

        // Detect Content-Encoding from response headers
        let content_encoding = response
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "identity".to_string());

        tracing::debug!(
            "Remote segment Content-Encoding: {} from {}",
            content_encoding,
            redact_uri(uri)
        );

        // Get response body
        let compressed_data = response
            .bytes()
            .await
            .map_err(|e| Error::InternalError(format!("Failed to read response body: {}", e)))?;

        // Decompress based on Content-Encoding header
        let decompressed_data = match content_encoding.to_lowercase().as_str() {
            "gzip" => {
                tracing::debug!("Decompressing gzip content");
                decompress_gzip(&compressed_data)?
            }
            "identity" | "" => compressed_data.to_vec(),
            other => {
                tracing::warn!(
                    "Unknown Content-Encoding '{}', treating as uncompressed",
                    other
                );
                compressed_data.to_vec()
            }
        };

        tracing::info!(
            "Successfully fetched remote spooled segment: {} bytes",
            decompressed_data.len()
        );

        Ok(decompressed_data)
    }

    /// Acknowledge a segment after successful download
    /// This is best-effort and non-fatal - failures are logged but don't prevent data retrieval
    async fn acknowledge_segment(
        &self,
        ack_uri: &str,
        headers: Option<&HashMap<String, Vec<String>>>,
    ) -> Result<()> {
        tracing::debug!("Acknowledging segment: {}", redact_uri(ack_uri));

        let mut request = self.http_client.post(ack_uri);

        // Apply headers if provided
        if let Some(headers_map) = headers {
            for (key, values) in headers_map {
                for value in values {
                    request = request.header(key, value);
                }
            }
        }

        let response = request.send().await.map_err(|e| {
            Error::InternalError(format!(
                "Failed to send acknowledgment to {}: {}",
                redact_uri(ack_uri),
                redact_reqwest_url(e)
            ))
        })?;

        if !response.status().is_success() {
            tracing::warn!(
                "Acknowledgment returned non-success status: {} for {}",
                response.status(),
                redact_uri(ack_uri)
            );
        }

        Ok(())
    }
}

/// Decompress gzip-compressed data
fn decompress_gzip(compressed_data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(compressed_data);
    let mut decompressed = Vec::new();

    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| Error::Decode(format!("Failed to decompress gzip data: {}", e)))?;

    Ok(decompressed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A presigned segment URL is a bearer credential for that object. It must
    /// never reach a log line or an error message intact.
    #[test]
    fn redact_uri_removes_the_presigned_query() {
        let signed = "http://minio:9003/spooling/segments/01KYRV.json%2Bzstd\
                      ?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=minioadmin\
                      &X-Amz-Signature=dec77808af5572babba07ca1d41b0858554897018be9d16ed53ad792072c87bb";
        let redacted = redact_uri(signed);
        assert_eq!(
            redacted,
            "http://minio:9003/spooling/segments/01KYRV.json%2Bzstd?<redacted>"
        );
        assert!(!redacted.contains("X-Amz-Signature"));
        assert!(!redacted.contains("dec77808"));
    }

    #[test]
    fn redact_uri_leaves_an_unsigned_uri_alone() {
        let plain = "http://storage.example.com/segments/abc.json";
        assert_eq!(redact_uri(plain), plain);
    }

    #[tokio::test]
    async fn a_failed_fetch_does_not_print_the_signature() {
        // 192.0.2.0/24 is TEST-NET-1, so this connect cannot succeed.
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_millis(200))
            .build()
            .unwrap();
        let fetcher = SegmentFetcher::new(client);
        let uri = "http://192.0.2.1:9003/spooling/segments/x.json\
                   ?X-Amz-Signature=SUPERSECRETSIGNATURE&X-Amz-Credential=minioadmin";

        let err = fetcher
            .fetch_spooled_segment(uri, None)
            .await
            .expect_err("connecting to TEST-NET-1 must fail");
        let rendered = format!("{err}");
        assert!(
            !rendered.contains("SUPERSECRETSIGNATURE"),
            "the presigned signature leaked into the error: {rendered}"
        );
        // The error class must survive redaction — `need_retry_fetch` matches
        // on it, so turning this into a stringly error would make every
        // transient segment failure terminal.
        assert!(
            matches!(err, Error::HttpError(_)),
            "expected Error::HttpError, got {err:?}"
        );
    }
}
