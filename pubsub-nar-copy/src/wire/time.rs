use bytes::{BufMut, BytesMut};
use nom::{IResult, Parser as _};

use crate::wire::{NixDeserialize, NixSerialize, ProtocolVersion};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Time(u64);

impl Time {
    pub fn new(time: u64) -> Time {
        Time(time)
    }
}

impl<'a> NixDeserialize<'a> for Time {
    fn deserialize(_protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, time) = nom::number::le_u64().parse(input)?;

        Ok((input, Self(time)))
    }
}

impl NixSerialize for Time {
    fn serialize(&self, _protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        bytes.put_u64_le(self.0);
    }
}
