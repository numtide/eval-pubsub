use bytes::{BufMut, BytesMut};
use nom::{IResult, Parser as _};

use crate::{
    ProtocolVersion,
    wire::{NixDeserialize, NixSerialize},
};

impl<I> NixDeserialize for Vec<I>
where
    I: NixDeserialize,
{
    fn deserialize(protocol_version: ProtocolVersion, input: &[u8]) -> IResult<&[u8], Vec<I>> {
        let (mut input, size) = nom::number::le_u64().parse(input)?;
        let mut set = Vec::with_capacity(size as usize);

        for _ in 0..size {
            let (new_input, item) = I::deserialize(protocol_version, input)?;
            input = new_input;
            set.push(item);
        }

        Ok((input, set))
    }
}

impl<I> NixSerialize for Vec<I>
where
    I: NixSerialize,
{
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        let size = self.len();

        bytes.put_u64_le(size as u64);

        for item in self {
            item.serialize(protocol_version, bytes)
        }
    }
}
