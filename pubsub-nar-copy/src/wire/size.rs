use nom::Parser as _;

use crate::wire::NixDeserialize;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Size(u64);

impl NixDeserialize for Size {
    fn deserialize(
        _protocol_version: super::ProtocolVersion,
        input: &[u8],
    ) -> nom::IResult<&[u8], Self> {
        let (input, size) = nom::number::le_u64().parse(input)?;

        Ok((input, Self(size)))
    }
}
