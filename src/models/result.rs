use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::*;
use crate::error::Error;
#[cfg(feature = "spooling")]
use crate::spooling::Segment;
use crate::Trino;

/// Query result data can be either Direct (inline array) or Spooled (compressed segments)
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum QueryResultData<T: Trino> {
    // Spooled protocol: data is an object with encoding and segments
    Spooled(SpooledData),
    // Direct protocol: data is a simple JSON array
    #[serde(bound(deserialize = "Vec<T>: Deserialize<'de>"))]
    Direct(Vec<T>),
}

/// Why a spooled page cannot be decoded from this type. The message differs by
/// feature so the caller is told the actionable thing in each build.
#[cfg(feature = "spooling")]
const SPOOLED_UNSUPPORTED: &str =
    "spooled query data cannot be decoded from QueryResultData: its segments may \
     live in object storage and decoding needs the query's column metadata, \
     neither of which this type has. Use Client::pager, Client::stream or \
     Client::get_all.";

#[cfg(not(feature = "spooling"))]
const SPOOLED_UNSUPPORTED: &str =
    "server sent spooled data but the 'spooling' feature is not enabled. Add \
     features = [\"spooling\"] to your trino-rust-client dependency in Cargo.toml.";

impl<T> QueryResultData<T>
where
    T: Trino,
    for<'de> T: serde::Deserialize<'de>,
{
    /// Rows carried inline by the Direct protocol.
    ///
    /// Spooled pages return [`Error::Protocol`] rather than rows. Resolving them
    /// requires the HTTP client (segments may live in object storage) and the
    /// query's column metadata, and this type has neither — use
    /// [`Client::pager`](crate::Client::pager), [`Client::stream`](crate::Client::stream)
    /// or [`Client::get_all`](crate::Client::get_all) for a query that may spool.
    pub fn into_vec(self) -> Result<Vec<T>, Error> {
        match self {
            QueryResultData::Direct(data) => Ok(data),
            QueryResultData::Spooled(_) => Err(Error::Protocol(SPOOLED_UNSUPPORTED.to_string())),
        }
    }
}

/// Spooled data contains encoding format and segment references
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpooledData {
    pub encoding: String,
    #[cfg(feature = "spooling")]
    pub segments: Vec<Segment>,
}

/// Metadata about spooled data segments
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataAttributes {
    pub rows_count: Option<u64>,
    pub segment_size: Option<u64>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// Trino query result
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult<T: Trino> {
    pub id: String,
    pub info_uri: String,
    pub partial_cancel_uri: Option<String>,
    pub next_uri: Option<String>,

    pub columns: Option<Vec<Column>>,

    #[serde(bound(deserialize = "Option<QueryResultData<T>>: Deserialize<'de>"))]
    pub data: Option<QueryResultData<T>>,

    pub error: Option<QueryError>,

    pub stats: Stat,
    pub warnings: Vec<Warning>,

    pub update_type: Option<String>,
    pub update_count: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::Row;

    #[test]
    fn into_vec_returns_direct_rows() {
        let data: QueryResultData<Row> =
            serde_json::from_str(r#"[["alice",1],["bob",2]]"#).unwrap();
        let rows = data.into_vec().expect("direct data decodes");
        assert_eq!(rows.len(), 2);
    }

    // A remote segment cannot be fetched here: this type has no HTTP client.
    #[test]
    fn into_vec_errors_on_remote_spooled_page() {
        let json = r#"{"encoding":"json+zstd","segments":[
            {"type":"spooled","uri":"http://minio:9000/s/0","metadata":{}}
        ]}"#;
        let data: QueryResultData<Row> = serde_json::from_str(json).unwrap();
        assert!(matches!(data, QueryResultData::Spooled(_)));
        assert!(
            matches!(data.into_vec(), Err(Error::Protocol(_))),
            "spooled pages must report a protocol error, never an empty Vec"
        );
    }

    // Inline segments error too: decoding them here would bypass TrinoTy and
    // could disagree with the column-directed decoding get_all/stream use.
    #[test]
    fn into_vec_errors_on_inline_spooled_page() {
        let json = r#"{"encoding":"json","segments":[
            {"type":"inline","data":"W1siYWxpY2UiLDFdXQ==","metadata":{}}
        ]}"#;
        let data: QueryResultData<Row> = serde_json::from_str(json).unwrap();
        assert!(matches!(data, QueryResultData::Spooled(_)));
        assert!(matches!(data.into_vec(), Err(Error::Protocol(_))));
    }
}
