use bytes::{BufMut, BytesMut};
use nom::IResult;

use crate::wire::{self, NixDeserialize, NixSerialize, ProtocolVersion, StorePath};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptStorePath {
    StorePath(StorePath),
    Nothing,
}

impl TryFrom<&str> for OptStorePath {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Ok(OptStorePath::Nothing)
        } else {
            StorePath::try_from(value).map(OptStorePath::StorePath)
        }
    }
}

impl From<OptStorePath> for Option<String> {
    fn from(value: OptStorePath) -> Self {
        match value {
            OptStorePath::StorePath(store_path) => Some((&store_path).into()),
            OptStorePath::Nothing => None,
        }
    }
}

impl<'a> NixDeserialize<'a> for OptStorePath {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, string) = String::deserialize(protocol_version, input)?;
        if string.is_empty() {
            Ok((input, OptStorePath::Nothing))
        } else {
            Ok((
                input,
                OptStorePath::StorePath(string.as_str().try_into().unwrap()),
            ))
        }
    }
}

impl NixSerialize for OptStorePath {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        match self {
            OptStorePath::StorePath(store_path) => store_path.serialize(protocol_version, bytes),
            OptStorePath::Nothing => bytes.put_u64_le(0),
        }
    }
}
