use std::array::TryFromSliceError;

use bytes::BytesMut;
use nix_base32::{from_nix_base32, to_nix_base32};
use nom::Parser as _;

use crate::wire::{self, NixDeserialize, NixSerialize};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct StorePathHash {
    pub hash: [u8; 20],
}

impl From<&StorePathHash> for String {
    fn from(value: &StorePathHash) -> Self {
        to_nix_base32(&value.hash)
    }
}

impl From<StorePathHash> for String {
    fn from(value: StorePathHash) -> Self {
        to_nix_base32(&value.hash)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct HashError;

impl std::fmt::Display for HashError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HashError")
    }
}
impl std::error::Error for HashError {}

impl TryFrom<String> for StorePathHash {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match from_nix_base32(&value) {
            Some(hash) => Ok(StorePathHash {
                hash: hash.try_into().map_err(|_| Box::new(HashError))?,
            }),
            None => Err(Box::new(HashError)),
        }
    }
}

impl TryFrom<&[u8]> for StorePathHash {
    type Error = TryFromSliceError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(StorePathHash {
            hash: value.try_into()?,
        })
    }
}

impl TryFrom<Vec<u8>> for StorePathHash {
    type Error = TryFromSliceError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(StorePathHash {
            hash: value.as_slice().try_into()?,
        })
    }
}

impl<'a> NixDeserialize<'a> for StorePathHash {
    fn deserialize(
        _protocol_version: wire::ProtocolVersion,
        input: &'a [u8],
    ) -> nom::IResult<&'a [u8], Self> {
        wire::bytes::with_bytes(
            |input| {
                let (input, hash) =
                    nom::bytes::complete::is_a("0123456789abcdfghijklmnpqrsvwxyz").parse(input)?;
                let hash: [u8; 20] = from_nix_base32(unsafe { str::from_utf8_unchecked(hash) })
                    .expect("a valid base32 encoded hash")
                    .try_into()
                    .expect("a hash of length 160 bits (20 bytes)");

                Ok((input, Self { hash }))
            },
            true,
        )
        .parse(input)
    }
}

impl NixSerialize for StorePathHash {
    fn serialize(&self, protocol_version: wire::ProtocolVersion, bytes: &mut BytesMut) {
        Into::<String>::into(self).serialize(protocol_version, bytes)
    }
}
