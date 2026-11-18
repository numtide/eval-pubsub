use crate::wire::{NixDeserialize, Size};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TraceLine {
    have_pos: Size,
    hint: String,
}

impl<'a> NixDeserialize<'a> for TraceLine {
    fn deserialize(
        protocol_version: super::ProtocolVersion,
        input: &'a [u8],
    ) -> nom::IResult<&'a [u8], Self> {
        let (input, have_pos) = Size::deserialize(protocol_version, input)?;
        let (input, hint) = String::deserialize(protocol_version, input)?;

        Ok((input, Self { have_pos, hint }))
    }
}
