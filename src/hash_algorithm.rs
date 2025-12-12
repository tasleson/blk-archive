use serde::{Deserialize, Serialize};

use crate::hash_dispatch::*;

/// Hash algorithms supported for block lookups
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
pub enum HashAlgorithmStored {
    #[default]
    #[serde(rename = "blake2b-256")]
    Blake2b256,
    #[serde(rename = "blake2b-128")]
    Blake2b128,
    #[serde(rename = "xxhash3-128")]
    XxHash3_128,
    #[serde(rename = "murmur3-128")]
    Murmur3_128,
    #[serde(rename = "blake3-256")]
    Blake3_256,
    #[serde(rename = "blake3-128")]
    Blake3_128,
}

impl HashAlgorithmStored {
    pub fn convert(&self) -> (HashAlgorithm, DigestSize) {
        match self {
            HashAlgorithmStored::Blake2b256 => (HashAlgorithm::Blake2b, DigestSize::Bits256),
            HashAlgorithmStored::Blake2b128 => (HashAlgorithm::Blake2b, DigestSize::Bits128),
            HashAlgorithmStored::XxHash3_128 => (HashAlgorithm::Xxh3, DigestSize::Bits128),
            HashAlgorithmStored::Murmur3_128 => (HashAlgorithm::Murmur3, DigestSize::Bits128),
            HashAlgorithmStored::Blake3_128 => (HashAlgorithm::Blake3, DigestSize::Bits128),
            HashAlgorithmStored::Blake3_256 => (HashAlgorithm::Blake3, DigestSize::Bits256),
        }
    }

    pub fn parse(s: &str) -> Option<HashAlgorithmStored> {
        match s {
            "blake3-128" => Some(HashAlgorithmStored::Blake2b128),
            "blake3-256" => Some(HashAlgorithmStored::Blake3_256),
            "blake2b-256" => Some(HashAlgorithmStored::Blake2b256),
            "blake2b-128" => Some(HashAlgorithmStored::Blake2b128),
            "xxhash3-128" => Some(HashAlgorithmStored::XxHash3_128),
            "murmur3-128" => Some(HashAlgorithmStored::Murmur3_128),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            HashAlgorithmStored::Blake2b256 => "blake2b-256",
            HashAlgorithmStored::Blake2b128 => "blake2b-128",
            HashAlgorithmStored::XxHash3_128 => "xxhash3-128",
            HashAlgorithmStored::Murmur3_128 => "murmur3-128",
            HashAlgorithmStored::Blake3_128 => "blake3-128",
            HashAlgorithmStored::Blake3_256 => "blake3-256",
        }
    }
}

#[cfg(test)]
mod tests {
    //use super::*;
}
