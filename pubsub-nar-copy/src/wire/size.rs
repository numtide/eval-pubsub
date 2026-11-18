use nom::Parser as _;

use crate::wire::NixDeserialize;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Size(u64);

impl<'a> NixDeserialize<'a> for Size {
    fn deserialize(
        _protocol_version: super::ProtocolVersion,
        input: &'a [u8],
    ) -> nom::IResult<&'a [u8], Self> {
        let (input, size) = nom::number::le_u64().parse(input)?;

        Ok((input, Self(size)))
    }
}
