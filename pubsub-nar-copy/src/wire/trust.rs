use bytes::{BufMut as _, BytesMut};
use nom::{IResult, Parser as _, number::Endianness};

use crate::wire::{NixDeserialize, NixSerialize, ProtocolVersion};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Trust {
    Trusted = 1,
    NotTrusted = 2,
}

impl TryFrom<u64> for Trust {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Trusted),
            2 => Ok(Self::NotTrusted),
            _ => Err(format!("invalid trust state {}", value)),
        }
    }
}

impl From<&Trust> for u64 {
    fn from(value: &Trust) -> Self {
        match value {
            Trust::Trusted => 1,
            Trust::NotTrusted => 2,
        }
    }
}

impl From<Trust> for u64 {
    fn from(value: Trust) -> Self {
        (&value).into()
    }
}

impl NixSerialize for Trust {
    fn serialize(&self, _protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        bytes.put_u64_le(self.into())
    }
}

impl NixDeserialize for Trust {
    fn deserialize(protocol_version: ProtocolVersion, input: &[u8]) -> IResult<&[u8], Self> {
        let (input, trust) = nom::number::le_u64().parse(input)?;

        Ok((input, Self::try_from(trust).unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let trust = Trust::Trusted;

        let mut trust_bytes = BytesMut::new();

        trust.serialize(ProtocolVersion::default(), &mut trust_bytes);

        let (_, trust_roundtrip) =
            Trust::deserialize(ProtocolVersion::default(), trust_bytes.as_ref()).unwrap();

        assert_eq!(trust_roundtrip, trust);
    }
}
