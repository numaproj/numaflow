//!  Compression and Decompression before writing or after reading from ISB.

use std::io::Read;

use crate::Result;
use crate::config::pipeline::isb::CompressionType;
use crate::error::Error;

/// Compress data based on the compression type.
pub(super) fn compress(compression_type: CompressionType, data: &[u8]) -> Result<Vec<u8>> {
    match compression_type {
        CompressionType::Gzip => {
            use flate2::Compression;
            use flate2::write::GzEncoder;
            use std::io::Write;

            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(data)
                .map_err(|e| Error::ISB(format!("Failed to compress message with gzip: {e}")))?;
            encoder
                .finish()
                .map_err(|e| Error::ISB(format!("Failed to finish gzip compression: {e}")))
        }
        CompressionType::Zstd => {
            use std::io::Write;
            use zstd::Encoder;

            let mut encoder = Encoder::new(Vec::new(), 3)
                .map_err(|e| Error::ISB(format!("Failed to create zstd encoder: {e:?}")))?;
            encoder
                .write_all(data)
                .map_err(|e| Error::ISB(format!("Failed to compress message with zstd: {e}")))?;
            encoder
                .finish()
                .map_err(|e| Error::ISB(format!("Failed to finish zstd compression: {e}")))
        }
        CompressionType::LZ4 => {
            use lz4::EncoderBuilder;
            use std::io::Write;

            let mut encoder = EncoderBuilder::new()
                .build(Vec::new())
                .map_err(|e| Error::ISB(format!("Failed to create lz4 encoder: {e:?}")))?;
            encoder
                .write_all(data)
                .map_err(|e| Error::ISB(format!("Failed to compress message with lz4: {e}")))?;
            let (compressed_data, _) = encoder.finish();
            Ok(compressed_data)
        }
        CompressionType::None => Ok(data.to_vec()),
    }
}

/// Decompress data based on the compression type.
pub(super) fn decompress(compression_type: CompressionType, data: &[u8]) -> Result<Vec<u8>> {
    match compression_type {
        CompressionType::Gzip => {
            use flate2::read::GzDecoder;

            let mut decoder: GzDecoder<&[u8]> = GzDecoder::new(data);
            let mut decompressed = vec![];
            decoder
                .read_to_end(&mut decompressed)
                .map_err(|e| Error::ISB(format!("Failed to decompress message: {e}")))?;
            Ok(decompressed)
        }
        CompressionType::Zstd => {
            let mut decoder: zstd::Decoder<'static, std::io::BufReader<&[u8]>> =
                zstd::Decoder::new(data)
                    .map_err(|e| Error::ISB(format!("Failed to create zstd decoder: {e:?}")))?;
            let mut decompressed = vec![];
            decoder
                .read_to_end(&mut decompressed)
                .map_err(|e| Error::ISB(format!("Failed to decompress message: {e}")))?;
            Ok(decompressed)
        }
        CompressionType::LZ4 => {
            let mut decoder: lz4::Decoder<&[u8]> = lz4::Decoder::new(data)
                .map_err(|e| Error::ISB(format!("Failed to create lz4 decoder: {e:?}")))?;
            let mut decompressed = vec![];
            decoder
                .read_to_end(&mut decompressed)
                .map_err(|e| Error::ISB(format!("Failed to decompress message: {e}")))?;
            Ok(decompressed)
        }
        CompressionType::None => Ok(data.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::isb::CompressionType;

    #[test]
    fn test_compress_decompress_gzip() {
        let test_data = b"Hello, World! This is a test message for gzip compression.";

        let compressed = compress(CompressionType::Gzip, test_data).unwrap();
        assert_ne!(compressed, test_data.to_vec());
        assert!(!compressed.is_empty());

        let decompressed = decompress(CompressionType::Gzip, &compressed).unwrap();
        assert_eq!(decompressed, test_data.to_vec());
    }

    #[test]
    fn test_compress_decompress_zstd() {
        let test_data = b"Hello, World! This is a test message for zstd compression.";

        let compressed = compress(CompressionType::Zstd, test_data).unwrap();
        assert_ne!(compressed, test_data.to_vec());
        assert!(!compressed.is_empty());

        let decompressed = decompress(CompressionType::Zstd, &compressed).unwrap();
        assert_eq!(decompressed, test_data.to_vec());
    }

    #[test]
    fn test_compress_decompress_lz4() {
        let test_data = b"Hello, World! This is a test message for lz4 compression.";

        let compressed = compress(CompressionType::LZ4, test_data).unwrap();
        assert_ne!(compressed, test_data.to_vec());
        assert!(!compressed.is_empty());

        let decompressed = decompress(CompressionType::LZ4, &compressed).unwrap();
        assert_eq!(decompressed, test_data.to_vec());
    }

    #[test]
    fn test_compress_decompress_empty_payload_gzip() {
        let test_data = vec![];

        let compressed = compress(CompressionType::Gzip, &test_data).unwrap();
        let decompressed = decompress(CompressionType::Gzip, &compressed).unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_compress_decompress_empty_payload_zstd() {
        let test_data = vec![];

        let compressed = compress(CompressionType::Zstd, &test_data).unwrap();
        let decompressed = decompress(CompressionType::Zstd, &compressed).unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_compress_decompress_empty_payload_lz4() {
        let test_data = vec![];

        let compressed = compress(CompressionType::LZ4, &test_data).unwrap();
        let decompressed = decompress(CompressionType::LZ4, &compressed).unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_decompress_invalid_lz4_data() {
        let invalid_data = b"This is not lz4 compressed data".to_vec();

        let result = decompress(CompressionType::LZ4, &invalid_data);
        assert!(result.is_err());

        if let Err(Error::ISB(msg)) = result {
            assert!(
                msg.contains("Failed to create lz4 decoder")
                    || msg.contains("Failed to decompress message")
            );
        } else {
            panic!("Expected ISB error with lz4 message");
        }
    }

    #[test]
    fn test_decompress_truncated_gzip_data() {
        let test_data = b"Hello, World! This is a test message for gzip compression.";

        // First compress the data properly
        let compressed = compress(CompressionType::Gzip, test_data).unwrap();

        // Then truncate the compressed data to simulate corruption
        let mut truncated_data = compressed;
        truncated_data.truncate(truncated_data.len() / 2);

        let result = decompress(CompressionType::Gzip, &truncated_data);

        assert!(result.is_err());
        if let Err(Error::ISB(msg)) = result {
            assert!(msg.contains("Failed to decompress message"));
        } else {
            panic!("Expected ISB error with decompression message");
        }
    }

    #[test]
    fn test_compress_decompress_binary_data_zstd() {
        // Create binary test data with various byte values
        let test_data: Vec<u8> = (0..=255).collect();

        let compressed = compress(CompressionType::Zstd, &test_data).unwrap();
        let decompressed = decompress(CompressionType::Zstd, &compressed).unwrap();

        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_round_trip_all_compression_types() {
        let test_data = b"This is a comprehensive test for all compression types with various characters: !@#$%^&*()_+-=[]{}|;':\",./<>?`~".to_vec();

        let compression_types = vec![
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Zstd,
            CompressionType::LZ4,
        ];

        for compression_type in compression_types {
            let compressed = compress(compression_type, &test_data).unwrap();
            let decompressed = decompress(compression_type, &compressed).unwrap();
            assert_eq!(
                decompressed, test_data,
                "Failed for compression type: {:?}",
                compression_type
            );
        }
    }
}
