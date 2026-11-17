use bytes::{BufMut, Bytes, BytesMut};
use nom::{IResult, Parser as _, number::Endianness};

use crate::wire::{NixDeserialize, NixSerialize, ProtocolVersion};

impl NixDeserialize for BytesMut {
    fn deserialize(protocol_version: ProtocolVersion, input: &[u8]) -> IResult<&[u8], Self> {
        let (input, vec) = <Vec<u8>>::deserialize(protocol_version, input)?;

        Ok((input, BytesMut::from_iter(vec)))
    }
}

impl NixDeserialize for Vec<u8> {
    fn deserialize(_protocol_version: ProtocolVersion, input: &[u8]) -> IResult<&[u8], Self> {
        let (input, size) = nom::number::u64(Endianness::Little).parse(input)?;
        let padding = calculate_padding(size);

        let mut buf = Vec::with_capacity((size + padding) as usize);

        let (input, bytes) = nom::bytes::take(size + padding).parse(input)?;
        buf.put_slice(&bytes[0..size as usize]);

        Ok((input, buf))
    }
}

impl NixDeserialize for Bytes {
    fn deserialize(protocol_version: ProtocolVersion, input: &[u8]) -> IResult<&[u8], Self> {
        let (input, bytes_mut) = BytesMut::deserialize(protocol_version, input)?;

        Ok((input, bytes_mut.freeze()))
    }
}

impl NixSerialize for BytesMut {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        <&[u8] as NixSerialize>::serialize(&self.as_ref(), protocol_version, bytes);
    }
}

impl NixSerialize for &[u8] {
    fn serialize(&self, _protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        let size = self.len();
        let padding = calculate_padding(size as u64);
        bytes.reserve(size_of::<u64>() + size);

        bytes.put_u64_le(self.len() as u64);
        bytes.put_slice(self);
        for _ in 0..padding {
            bytes.put_u8(0);
        }
    }
}

pub fn calculate_padding(size: u64) -> u64 {
    ((size as usize + 7) & !7usize) as u64 - size
}
