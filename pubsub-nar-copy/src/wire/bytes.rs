use std::marker::PhantomData;

use bytes::{BufMut, Bytes, BytesMut};
use nom::{IResult, Parser as _, number::Endianness};

use crate::wire::{NixDeserialize, NixSerialize, ProtocolVersion};

pub struct Unpadded<A>(pub A);

fn deserialize_vec_u8(
    _protocol_version: ProtocolVersion,
    input: &[u8],
    padding: bool,
) -> IResult<&[u8], Vec<u8>> {
    let (input, size) = nom::number::u64(Endianness::Little).parse(input)?;
    let padding = if padding { calculate_padding(size) } else { 0 };

    let mut buf = Vec::with_capacity((size + padding) as usize);

    let (input, bytes) = nom::bytes::take(size + padding).parse(input)?;
    buf.put_slice(&bytes[0..size as usize]);

    Ok((input, buf))
}

impl<'a> NixDeserialize<'a> for BytesMut {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, vec) = <Vec<u8>>::deserialize(protocol_version, input)?;

        Ok((input, BytesMut::from_iter(vec)))
    }
}

impl<'a> NixDeserialize<'a> for Vec<u8> {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        deserialize_vec_u8(protocol_version, input, true)
    }
}

impl<'a> NixDeserialize<'a> for Bytes {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, bytes_mut) = BytesMut::deserialize(protocol_version, input)?;

        Ok((input, bytes_mut.freeze()))
    }
}

impl<'a> NixDeserialize<'a> for Unpadded<BytesMut> {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, vec) = <Unpadded<Vec<u8>>>::deserialize(protocol_version, input)?;

        Ok((input, Unpadded(BytesMut::from_iter(vec.0))))
    }
}

impl<'a> NixDeserialize<'a> for Unpadded<Vec<u8>> {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, vec) = deserialize_vec_u8(protocol_version, input, false)?;

        Ok((input, Unpadded(vec)))
    }
}

impl<'a> NixDeserialize<'a> for Unpadded<Bytes> {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, bytes_mut) = <Unpadded<BytesMut>>::deserialize(protocol_version, input)?;

        Ok((input, Unpadded(bytes_mut.0.freeze())))
    }
}

fn serialize_slice_u8(
    value: &[u8],
    _protocol_version: ProtocolVersion,
    bytes: &mut BytesMut,
    padding: bool,
) {
    let size = value.len();
    let padding = if padding {
        calculate_padding(size as u64)
    } else {
        0
    };
    bytes.reserve(size_of::<u64>() + size + padding as usize);

    log::debug!("sending {size} bytes with {padding} bytes padding");
    bytes.put_u64_le(size as u64);
    bytes.put_slice(value);
    for _ in 0..padding {
        assert!(padding != 0);
        bytes.put_u8(0);
    }
}

impl NixSerialize for BytesMut {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        <&[u8] as NixSerialize>::serialize(&self.as_ref(), protocol_version, bytes);
    }
}

impl NixSerialize for &[u8] {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        serialize_slice_u8(self, protocol_version, bytes, true)
    }
}

impl NixSerialize for Unpadded<BytesMut> {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        <Unpadded<&[u8]> as NixSerialize>::serialize(
            &Unpadded(self.0.as_ref()),
            protocol_version,
            bytes,
        );
    }
}

impl NixSerialize for Unpadded<&[u8]> {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        serialize_slice_u8(self.0, protocol_version, bytes, false)
    }
}

pub fn calculate_padding(size: u64) -> u64 {
    ((size as usize + 7) & !7usize) as u64 - size
}
