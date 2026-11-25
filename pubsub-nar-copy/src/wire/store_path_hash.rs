use std::array::TryFromSliceError;

use bytes::BytesMut;
use nix_base32::{from_nix_base32, to_nix_base32};

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
        protocol_version: wire::ProtocolVersion,
        input: &'a [u8],
    ) -> nom::IResult<&'a [u8], Self> {
        let (input, string) = String::deserialize(protocol_version, input)?;
        let hash = Self::try_from(string).unwrap();

        Ok((input, hash))
    }
}

impl NixSerialize for StorePathHash {
    fn serialize(&self, protocol_version: wire::ProtocolVersion, bytes: &mut BytesMut) {
        Into::<String>::into(self).serialize(protocol_version, bytes)
    }
}
