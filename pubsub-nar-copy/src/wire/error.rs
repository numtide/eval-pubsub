use crate::wire::{NixDeserialize, TraceLine, size::Size, verbosity::Verbosity};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NixError {
    r#type: String,
    level: Verbosity,
    name: String,
    msg: String,
    have_pos: Size,
    traces: Vec<TraceLine>,
}

impl NixDeserialize for NixError {
    fn deserialize(
        protocol_version: super::ProtocolVersion,
        input: &[u8],
    ) -> nom::IResult<&[u8], Self> {
        let (input, r#type) = String::deserialize(protocol_version, input)?;
        let (input, level) = Verbosity::deserialize(protocol_version, input)?;
        let (input, name) = String::deserialize(protocol_version, input)?;
        let (input, msg) = String::deserialize(protocol_version, input)?;
        let (input, have_pos) = Size::deserialize(protocol_version, input)?;
        let (input, traces) = <Vec<TraceLine>>::deserialize(protocol_version, input)?;

        Ok((
            input,
            Self {
                r#type,
                level,
                name,
                msg,
                have_pos,
                traces,
            },
        ))
    }
}
