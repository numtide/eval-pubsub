use std::{
    marker::PhantomData,
    num::{NonZeroU64, NonZeroUsize},
};

use bytes::{BufMut, Bytes, BytesMut};
use nom::{IResult, Mode as _, Parser as _, error::ParseError as _, number::Endianness};

use crate::wire::{NixDeserialize, NixSerialize, ProtocolVersion};

pub struct Unpadded<A>(pub A);

pub struct WithBytes<F> {
    parser: F,
    padding: bool,
}

pub fn with_bytes<I, O, E: nom::error::ParseError<I>, F>(
    parser: F,
    padding: bool,
) -> impl nom::Parser<I, Output = O, Error = E>
where
    F: nom::Parser<I, Error = E, Output = O>,
    I: nom::Input<Item = u8>,
{
    WithBytes { parser, padding }
}

impl<I, F> nom::Parser<I> for WithBytes<F>
where
    I: nom::Input<Item = u8>,
    F: nom::Parser<I>,
    F::Error: nom::error::ParseError<I>,
{
    type Output = F::Output;
    type Error = F::Error;

    fn process<OM>(&mut self, input: I) -> nom::PResult<OM, I, Self::Output, Self::Error>
    where
        OM: nom::OutputMode,
    {
        let (i, mut size) = nom::number::u64(Endianness::Little).process::<nom::OutputM<
            nom::Emit,
            OM::Error,
            OM::Incomplete,
        >>(input)?;

        let padding = if self.padding {
            calculate_padding(size)
        } else {
            0
        };
        size += padding;

        if let Some(needed) = (size as usize)
            .checked_sub(i.input_len())
            .and_then(NonZeroUsize::new)
        {
            Err(nom::Err::Incomplete(nom::Needed::Size(needed)))
        } else {
            let (rest, i) = i.take_split(size as usize - padding as usize);

            let (input_inner, value) = self.parser.process::<OM>(i)?;
            if input_inner.input_len() != 0 as usize {
                return Err(nom::Err::Error(OM::Error::bind(|| {
                    <F as nom::Parser<I>>::Error::from_error_kind(
                        input_inner,
                        nom::error::ErrorKind::SeparatedList,
                    )
                })));
            } else {
                Ok((rest.take_from(padding as usize), value))
            }
        }
    }
}

fn deserialize_slice(
    _protocol_version: ProtocolVersion,
    input: &[u8],
    padding: bool,
) -> IResult<&[u8], &[u8]> {
    let (input, size) = nom::number::u64(Endianness::Little).parse(input)?;
    let padding = if padding { calculate_padding(size) } else { 0 };

    let (input, bytes) = nom::bytes::take(size + padding).parse(input)?;

    Ok((input, &bytes[0..size as usize]))
}

impl<'a> NixDeserialize<'a> for &'a [u8] {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        deserialize_slice(protocol_version, input, true)
    }
}

impl<'a> NixDeserialize<'a> for BytesMut {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, vec) = <Vec<u8>>::deserialize(protocol_version, input)?;

        Ok((input, BytesMut::from_iter(vec)))
    }
}

impl<'a> NixDeserialize<'a> for Vec<u8> {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, slice) = deserialize_slice(protocol_version, input, true)?;
        Ok((input, slice.to_owned()))
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
        let (input, slice) = deserialize_slice(protocol_version, input, false)?;
        Ok((input, Unpadded(slice.to_owned())))
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
