use anyhow::{anyhow, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Write};

use crate::config::StreamConfig;

const STREAM_METADATA_VERSION: u32 = 1;

/// Serialize a StreamConfig into binary format for storage in a slab
pub fn serialize_stream_config(cfg: &StreamConfig) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    // Write version first
    buf.write_u32::<LittleEndian>(STREAM_METADATA_VERSION)?;

    // Write name
    if let Some(name) = &cfg.name {
        buf.write_u32::<LittleEndian>(name.len() as u32)?;
        buf.write_all(name.as_bytes())?;
    } else {
        buf.write_u32::<LittleEndian>(0)?;
    }

    // Write source_path
    buf.write_u32::<LittleEndian>(cfg.source_path.len() as u32)?;
    buf.write_all(cfg.source_path.as_bytes())?;

    // Write pack_time (convert from RFC3339 string to Unix timestamp nanos)
    let pack_time = chrono::DateTime::parse_from_rfc3339(&cfg.pack_time)
        .context("Failed to parse pack_time")?
        .timestamp_nanos_opt()
        .context("Timestamp out of range")?;
    buf.write_i64::<LittleEndian>(pack_time)?;

    // Write size fields
    buf.write_u64::<LittleEndian>(cfg.size)?;
    buf.write_u64::<LittleEndian>(cfg.mapped_size)?;
    buf.write_u64::<LittleEndian>(cfg.packed_size)?;

    // Write thin_id (0xFFFFFFFF means None)
    buf.write_u32::<LittleEndian>(cfg.thin_id.unwrap_or(0xFFFFFFFF))?;

    // Write source_sig
    if let Some(sig) = &cfg.source_sig {
        buf.write_u32::<LittleEndian>(sig.len() as u32)?;
        buf.write_all(sig.as_bytes())?;
    } else {
        buf.write_u32::<LittleEndian>(0)?;
    }

    // Write mapping location
    buf.write_u32::<LittleEndian>(cfg.first_mapping_slab)?;
    buf.write_u32::<LittleEndian>(cfg.num_mapping_slabs)?;

    Ok(buf)
}

/// Deserialize a StreamConfig from binary format
pub fn deserialize_stream_config(data: &[u8]) -> Result<StreamConfig> {
    let mut cursor = Cursor::new(data);

    // Read and validate version
    let version = cursor.read_u32::<LittleEndian>()?;
    if version != STREAM_METADATA_VERSION {
        return Err(anyhow!(
            "Unsupported stream metadata version: {} (expected {})",
            version,
            STREAM_METADATA_VERSION
        ));
    }

    // Read name
    let name_len = cursor.read_u32::<LittleEndian>()? as usize;
    let name = if name_len > 0 {
        let mut buf = vec![0u8; name_len];
        cursor.read_exact(&mut buf)?;
        Some(String::from_utf8(buf).context("Invalid UTF-8 in name")?)
    } else {
        None
    };

    // Read source_path
    let path_len = cursor.read_u32::<LittleEndian>()? as usize;
    let mut path_buf = vec![0u8; path_len];
    cursor.read_exact(&mut path_buf)?;
    let source_path = String::from_utf8(path_buf).context("Invalid UTF-8 in source_path")?;

    // Read pack_time (convert from Unix timestamp nanos back to RFC3339 string)
    let pack_time_nanos = cursor.read_i64::<LittleEndian>()?;
    let dt = chrono::DateTime::from_timestamp(
        pack_time_nanos / 1_000_000_000,
        (pack_time_nanos % 1_000_000_000) as u32,
    )
    .context("Invalid timestamp")?;
    let pack_time = dt.to_rfc3339();

    // Read size fields
    let size = cursor.read_u64::<LittleEndian>()?;
    let mapped_size = cursor.read_u64::<LittleEndian>()?;
    let packed_size = cursor.read_u64::<LittleEndian>()?;

    // Read thin_id
    let thin_id_raw = cursor.read_u32::<LittleEndian>()?;
    let thin_id = if thin_id_raw == 0xFFFFFFFF {
        None
    } else {
        Some(thin_id_raw)
    };

    // Read source_sig
    let sig_len = cursor.read_u32::<LittleEndian>()? as usize;
    let source_sig = if sig_len > 0 {
        let mut buf = vec![0u8; sig_len];
        cursor.read_exact(&mut buf)?;
        Some(String::from_utf8(buf).context("Invalid UTF-8 in source_sig")?)
    } else {
        None
    };

    // Read mapping location
    let first_mapping_slab = cursor.read_u32::<LittleEndian>()?;
    let num_mapping_slabs = cursor.read_u32::<LittleEndian>()?;

    Ok(StreamConfig {
        name,
        source_path,
        pack_time,
        size,
        mapped_size,
        packed_size,
        thin_id,
        source_sig,
        first_mapping_slab,
        num_mapping_slabs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() -> Result<()> {
        let original = StreamConfig {
            name: Some("test_stream".to_string()),
            source_path: "/dev/test".to_string(),
            pack_time: "2025-10-30T12:00:00+00:00".to_string(),
            size: 1024 * 1024 * 1024,
            mapped_size: 512 * 1024 * 1024,
            packed_size: 256 * 1024 * 1024,
            thin_id: Some(42),
            source_sig: Some("abc123".to_string()),
            first_mapping_slab: 10,
            num_mapping_slabs: 5,
        };

        let serialized = serialize_stream_config(&original)?;
        let deserialized = deserialize_stream_config(&serialized)?;

        assert_eq!(original.name, deserialized.name);
        assert_eq!(original.source_path, deserialized.source_path);
        assert_eq!(original.size, deserialized.size);
        assert_eq!(original.mapped_size, deserialized.mapped_size);
        assert_eq!(original.packed_size, deserialized.packed_size);
        assert_eq!(original.thin_id, deserialized.thin_id);
        assert_eq!(original.source_sig, deserialized.source_sig);
        assert_eq!(original.first_mapping_slab, deserialized.first_mapping_slab);
        assert_eq!(original.num_mapping_slabs, deserialized.num_mapping_slabs);

        Ok(())
    }

    #[test]
    fn test_optional_fields() -> Result<()> {
        let original = StreamConfig {
            name: None,
            source_path: "/dev/test".to_string(),
            pack_time: "2025-10-30T12:00:00+00:00".to_string(),
            size: 1024,
            mapped_size: 512,
            packed_size: 256,
            thin_id: None,
            source_sig: None,
            first_mapping_slab: 0,
            num_mapping_slabs: 1,
        };

        let serialized = serialize_stream_config(&original)?;
        let deserialized = deserialize_stream_config(&serialized)?;

        assert_eq!(original.name, deserialized.name);
        assert_eq!(original.thin_id, deserialized.thin_id);
        assert_eq!(original.source_sig, deserialized.source_sig);

        Ok(())
    }

    #[test]
    fn test_version_check() {
        let mut bad_data = vec![0u8; 4];
        bad_data[0] = 99; // Invalid version

        let result = deserialize_stream_config(&bad_data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("version"));
    }
}
