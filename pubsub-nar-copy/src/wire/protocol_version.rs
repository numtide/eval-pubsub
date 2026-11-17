use std::fmt::Display;

use bytes::{BufMut, BytesMut};
use nom::{IResult, Parser as _, number::Endianness};

use crate::wire::{NixDeserialize, NixSerialize};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord)]
pub struct ProtocolVersion {
    major: u8,
    minor: u8,
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        Self {
            major: 1,
            minor: 35,
        }
    }
}

impl TryFrom<u64> for ProtocolVersion {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let major = (value >> 8 & 0xff) as u8;
        let minor = (value & 0xff) as u8;

        if major != 1 {
            Err(String::from("protocol version major isn't 1"))
        } else {
            Ok(Self { major, minor })
        }
    }
}

impl From<ProtocolVersion> for u64 {
    fn from(value: ProtocolVersion) -> Self {
        (&value).into()
    }
}

impl From<&ProtocolVersion> for u64 {
    fn from(value: &ProtocolVersion) -> Self {
        (value.major as u64) << 8 | value.minor as u64
    }
}

impl PartialOrd for ProtocolVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.major.partial_cmp(&other.major) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.minor.partial_cmp(&other.minor)
    }
}

impl Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

impl ProtocolVersion {
    pub const fn from_parts(major: u8, minor: u8) -> Self {
        ProtocolVersion { major, minor }
    }
}

impl NixDeserialize for ProtocolVersion {
    fn deserialize(_protocol_version: ProtocolVersion, input: &[u8]) -> IResult<&[u8], Self> {
        let (input, major_minor) = nom::number::u64(Endianness::Little).parse(input)?;

        Ok((input, Self::try_from(major_minor).unwrap()))
    }
}

impl NixSerialize for ProtocolVersion {
    fn serialize(&self, _protocol_version: Self, bytes: &mut BytesMut) {
        bytes.put_u64_le(self.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_roundtrip() {
        let pv = ProtocolVersion::from_parts(1, 35);
        let num: u64 = pv.into();

        assert_eq!(pv, num.try_into().unwrap());
    }
}
