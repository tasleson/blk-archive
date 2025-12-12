/*
use crate::iovec::*;
use blake2::{Blake2b, Digest};
use std::convert::TryInto;

//-----------------------------------------
type Blake2b32 = Blake2b<generic_array::typenum::U4>;
type Blake2b64 = Blake2b<generic_array::typenum::U8>;
type Blake2b128 = Blake2b<generic_array::typenum::U16>;
type Blake2b256 = Blake2b<generic_array::typenum::U32>;

pub type Hash32 = generic_array::GenericArray<u8, generic_array::typenum::U4>;
pub type Hash64 = generic_array::GenericArray<u8, generic_array::typenum::U8>;
pub type Hash128 = generic_array::GenericArray<u8, generic_array::typenum::U16>;
pub type Hash256 = generic_array::GenericArray<u8, generic_array::typenum::U32>;

pub fn hash_256_iov(iov: &IoVec) -> Hash256 {
    let mut hasher = Blake2b256::new();
    for v in iov {
        hasher.update(&v[..]);
    }
    hasher.finalize()
}

pub fn hash_64_iov(iov: &IoVec) -> Hash64 {
    let mut hasher = Blake2b64::new();
    for v in iov {
        hasher.update(&v[..]);
    }
    hasher.finalize()
}

pub fn hash_32_iov(iov: &IoVec) -> Hash32 {
    let mut hasher = Blake2b32::new();
    for v in iov {
        hasher.update(&v[..]);
    }
    hasher.finalize()
}

pub fn hash_256(v: &[u8]) -> Hash256 {
    let mut hasher = Blake2b256::new();
    hasher.update(v);
    hasher.finalize()
}

pub fn hash_64(v: &[u8]) -> Hash64 {
    let mut hasher = Blake2b64::new();
    hasher.update(v);
    hasher.finalize()
}

pub fn hash_32(v: &[u8]) -> Hash32 {
    let mut hasher = Blake2b32::new();
    hasher.update(v);
    hasher.finalize()
}

pub fn hash_le_u64(h: &[u8]) -> u64 {
    let mini_hash = hash_64(h);
    u64::from_le_bytes(
        mini_hash[..8]
            .try_into()
            .expect("hash_64 must return at least 8 bytes"),
    )
}

// New configurable hash functions

pub fn hash_128_iov(iov: &IoVec) -> Hash128 {
    let mut hasher = Blake2b128::new();
    for v in iov {
        hasher.update(&v[..]);
    }
    hasher.finalize()
}

pub fn hash_128(v: &[u8]) -> Hash128 {
    let mut hasher = Blake2b128::new();
    hasher.update(v);
    hasher.finalize()
}
*/
