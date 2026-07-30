use crate::error::Error;
use crate::spooling::SpoolingEncoding;
use base64::prelude::*;
use std::convert::TryFrom;
use std::io::Read;
use zstd::stream::Decoder;
/// Decompress already-decoded segment bytes based on encoding.
///
/// `uncompressed_size` is the segment's `uncompressedSize` metadata attribute.
/// The `json+lz4` encoding needs it: Trino compresses a segment as a **raw LZ4
/// block**, which carries no length header of its own, so the output size has to
/// be known before decompression starts. `json` and `json+zstd` ignore it.
pub fn decompress_segment_bytes(
    compressed_data: &[u8],
    encoding: &SpoolingEncoding,
    uncompressed_size: Option<usize>,
) -> Result<String, Error> {
    decompress_bytes_internal(compressed_data, encoding, uncompressed_size)
}

/// Decode and decompress inline segment data.
///
/// `uncompressed_size` is as in [`decompress_segment_bytes`].
pub fn decode_inline_segment(
    encoded_data: &str,
    encoding: &str,
    uncompressed_size: Option<usize>,
) -> Result<String, Error> {
    let encoding = SpoolingEncoding::try_from(encoding)?;

    let compressed_data = BASE64_STANDARD
        .decode(encoded_data)
        .map_err(|e| Error::Decode(format!("Failed to base64 decode segment: {}", e)))?;

    decompress_bytes_internal(&compressed_data, &encoding, uncompressed_size)
}

/// Internal helper to decompress bytes with fallback logic
/// If the decompression fails, we fallback to plain JSON
/// If the fallback fails, we return the error
fn decompress_bytes_internal(
    compressed_data: &[u8],
    encoding: &SpoolingEncoding,
    uncompressed_size: Option<usize>,
) -> Result<String, Error> {
    let decompressed_data = match encoding {
        SpoolingEncoding::JsonZstd => decompress_zstd(compressed_data).or_else(|e| {
            let fallback_result = String::from_utf8(compressed_data.to_vec()).map_err(|utf8_err| {
                Error::Decode(format!(
                    "Failed to decompress zstd and plain JSON fallback also failed: {}, {}",
                    e, utf8_err
                ))
            });
            fallback_result
        })?,
        SpoolingEncoding::JsonLz4 => {
            decompress_lz4(compressed_data, uncompressed_size).or_else(|e| {
                String::from_utf8(compressed_data.to_vec()).map_err(|utf8_err| {
                    Error::Decode(format!(
                        "Failed to decompress lz4 and plain JSON fallback also failed: {}, {}",
                        e, utf8_err
                    ))
                })
            })?
        }
        SpoolingEncoding::Json => String::from_utf8(compressed_data.to_vec()).map_err(|e| {
            Error::Decode(format!(
                "Failed to convert uncompressed data to UTF-8: {}",
                e
            ))
        })?,
    };

    Ok(decompressed_data)
}

/// Decompress zstd-compressed data
fn decompress_zstd(compressed_data: &[u8]) -> Result<String, Error> {
    let mut decoder = match Decoder::new(compressed_data) {
        Ok(d) => d,
        Err(e) => {
            return Err(Error::InternalError(format!(
                "Failed to create zstd decoder: {}",
                e
            )));
        }
    };

    let mut decompressed = String::new();
    match decoder.read_to_string(&mut decompressed) {
        Ok(_) => Ok(decompressed),
        Err(e) => Err(Error::Decode(format!(
            "Failed to decompress zstd data: {}",
            e
        ))),
    }
}

/// Decompress a raw LZ4 block, the form Trino's `json+lz4` encoding produces.
///
/// Trino compresses a segment with airlift's `Lz4Compressor`, which emits a bare
/// LZ4 block: no frame magic (`04 22 4d 18`) and no length prefix. Measured
/// against a live coordinator, a segment starts straight at the first token byte.
/// A raw block therefore cannot be decompressed without being told the output
/// size, which is what `uncompressedSize` in the segment metadata is for.
fn decompress_lz4(
    compressed_data: &[u8],
    uncompressed_size: Option<usize>,
) -> Result<String, Error> {
    let size = uncompressed_size.ok_or_else(|| {
        Error::Decode(
            "Cannot decompress a json+lz4 segment without its uncompressedSize \
             metadata: a raw LZ4 block carries no length of its own"
                .to_string(),
        )
    })?;
    let size = i32::try_from(size).map_err(|_| {
        Error::Decode(format!(
            "Segment uncompressedSize {} does not fit in the i32 the LZ4 block \
             decompressor takes",
            size
        ))
    })?;

    let decompressed = lz4::block::decompress(compressed_data, Some(size))
        .map_err(|e| Error::Decode(format!("Failed to decompress lz4 data: {}", e)))?;

    String::from_utf8(decompressed)
        .map_err(|e| Error::Decode(format!("Decompressed lz4 data is not UTF-8: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_uncompressed_json() {
        // "[[1,2],[3,4]]" as base64
        let encoded = "W1sxLDJdLFszLDRdXQ==";
        let result = decode_inline_segment(encoded, "json", None).unwrap();
        assert_eq!(result, "[[1,2],[3,4]]");
    }

    #[test]
    fn test_decode_zstd_compressed() {
        // Create a zstd-compressed version of "[[1,2],[3,4]]"
        let original = "[[1,2],[3,4]]";
        let compressed = zstd::encode_all(original.as_bytes(), 3).unwrap();
        let encoded = BASE64_STANDARD.encode(&compressed);

        let result = decode_inline_segment(&encoded, "json+zstd", Some(original.len())).unwrap();
        assert_eq!(result, original);
    }

    /// Trino emits a **raw LZ4 block**: no frame magic and no length prefix, so
    /// the segment's `uncompressedSize` is what sizes the output. `prepend_size =
    /// false` is what makes the fixture match the wire form.
    #[test]
    fn test_decode_lz4_compressed() {
        let original = "[[1,2],[3,4]]";
        let compressed = lz4::block::compress(original.as_bytes(), None, false).unwrap();
        let encoded = BASE64_STANDARD.encode(&compressed);

        let result = decode_inline_segment(&encoded, "json+lz4", Some(original.len())).unwrap();
        assert_eq!(result, original);
    }

    /// A raw LZ4 block carries no length, so without the metadata there is
    /// nothing to size the output buffer from. Reported rather than guessed.
    #[test]
    fn test_lz4_without_uncompressed_size_is_an_error() {
        let original = "[[1,2],[3,4]]";
        let compressed = lz4::block::compress(original.as_bytes(), None, false).unwrap();
        let encoded = BASE64_STANDARD.encode(&compressed);

        let err = decode_inline_segment(&encoded, "json+lz4", None).unwrap_err();
        assert!(
            err.to_string().contains("uncompressedSize"),
            "the error must name the missing metadata, got: {err}"
        );
    }

    #[test]
    fn test_invalid_base64() {
        let result = decode_inline_segment("not!valid!base64!", "json", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_unsupported_encoding() {
        let encoded = "W1sxLDJdXQ==";
        let result = decode_inline_segment(encoded, "unknown", None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported spooling encoding"));
    }

    #[test]
    fn test_zstd_fallback_to_plain_json() {
        let plain_json = "[[2,\"data\"]]";
        let encoded = BASE64_STANDARD.encode(plain_json.as_bytes());
        let result = decode_inline_segment(&encoded, "json+zstd", Some(plain_json.len())).unwrap();
        assert_eq!(result, plain_json);
    }

    #[test]
    fn test_lz4_fallback_to_plain_json() {
        let plain_json = "[[1,\"test\"]]";
        let encoded = BASE64_STANDARD.encode(plain_json.as_bytes());
        let result = decode_inline_segment(&encoded, "json+lz4", Some(plain_json.len())).unwrap();
        assert_eq!(result, plain_json);
    }
}
