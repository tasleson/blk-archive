use serde::{Deserialize, Serialize};

/// Hash algorithms supported for block lookups
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
pub enum BlockHashAlgorithm {
    #[default]
    #[serde(rename = "blake2b-256")]
    Blake2b256,
    #[serde(rename = "blake2b-128")]
    Blake2b128,
    #[serde(rename = "xxhash3-128")]
    XxHash3_128,
    #[serde(rename = "murmur3-128")]
    Murmur3_128,
}

impl BlockHashAlgorithm {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "blake2b-256" => Some(BlockHashAlgorithm::Blake2b256),
            "blake2b-128" => Some(BlockHashAlgorithm::Blake2b128),
            "xxhash3-128" => Some(BlockHashAlgorithm::XxHash3_128),
            "murmur3-128" => Some(BlockHashAlgorithm::Murmur3_128),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            BlockHashAlgorithm::Blake2b256 => "blake2b-256",
            BlockHashAlgorithm::Blake2b128 => "blake2b-128",
            BlockHashAlgorithm::XxHash3_128 => "xxhash3-128",
            BlockHashAlgorithm::Murmur3_128 => "murmur3-128",
        }
    }
}

/// Hash algorithms supported for file/stream hashing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
pub enum FileHashAlgorithm {
    #[default]
    #[serde(rename = "blake3-256")]
    Blake3_256,
    #[serde(rename = "blake2b-256")]
    Blake2b256,
    #[serde(rename = "blake2b-128")]
    Blake2b128,
    #[serde(rename = "xxhash3-128")]
    XxHash3_128,
    #[serde(rename = "murmur3-128")]
    Murmur3_128,
}

impl FileHashAlgorithm {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "blake3-256" => Some(FileHashAlgorithm::Blake3_256),
            "blake2b-256" => Some(FileHashAlgorithm::Blake2b256),
            "blake2b-128" => Some(FileHashAlgorithm::Blake2b128),
            "xxhash3-128" => Some(FileHashAlgorithm::XxHash3_128),
            "murmur3-128" => Some(FileHashAlgorithm::Murmur3_128),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            FileHashAlgorithm::Blake3_256 => "blake3-256",
            FileHashAlgorithm::Blake2b256 => "blake2b-256",
            FileHashAlgorithm::Blake2b128 => "blake2b-128",
            FileHashAlgorithm::XxHash3_128 => "xxhash3-128",
            FileHashAlgorithm::Murmur3_128 => "murmur3-128",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_hash_parse() {
        assert_eq!(
            BlockHashAlgorithm::parse("blake2b-128"),
            Some(BlockHashAlgorithm::Blake2b128)
        );
        assert_eq!(
            BlockHashAlgorithm::parse("xxhash3-128"),
            Some(BlockHashAlgorithm::XxHash3_128)
        );
        assert_eq!(
            BlockHashAlgorithm::parse("murmur3-128"),
            Some(BlockHashAlgorithm::Murmur3_128)
        );
        assert_eq!(BlockHashAlgorithm::parse("invalid"), None);
    }

    #[test]
    fn test_file_hash_parse() {
        assert_eq!(
            FileHashAlgorithm::parse("blake3-256"),
            Some(FileHashAlgorithm::Blake3_256)
        );
        assert_eq!(
            FileHashAlgorithm::parse("blake2b-256"),
            Some(FileHashAlgorithm::Blake2b256)
        );
        assert_eq!(FileHashAlgorithm::parse("invalid"), None);
    }

    #[test]
    fn test_serde_block_hash() {
        let alg = BlockHashAlgorithm::XxHash3_128;
        let serialized = serde_yaml_ng::to_string(&alg).unwrap();
        let deserialized: BlockHashAlgorithm = serde_yaml_ng::from_str(&serialized).unwrap();
        assert_eq!(alg, deserialized);
    }

    #[test]
    fn test_serde_file_hash() {
        let alg = FileHashAlgorithm::Blake3_256;
        let serialized = serde_yaml_ng::to_string(&alg).unwrap();
        let deserialized: FileHashAlgorithm = serde_yaml_ng::from_str(&serialized).unwrap();
        assert_eq!(alg, deserialized);
    }
}
