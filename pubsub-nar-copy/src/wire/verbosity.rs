use nom::Parser as _;

use crate::wire::NixDeserialize;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Verbosity {
    Error = 0,
    Warn = 1,
    Notice = 2,
    Info = 3,
    Talkative = 4,
    Chatty = 5,
    Debug = 6,
    Vomit = 7,
}

impl From<Verbosity> for u64 {
    fn from(value: Verbosity) -> Self {
        match value {
            Verbosity::Error => 0,
            Verbosity::Warn => 1,
            Verbosity::Notice => 2,
            Verbosity::Info => 3,
            Verbosity::Talkative => 4,
            Verbosity::Chatty => 5,
            Verbosity::Debug => 6,
            Verbosity::Vomit => 7,
        }
    }
}

impl TryFrom<u64> for Verbosity {
    type Error = ();

    fn try_from(value: u64) -> Result<Self, <Verbosity as TryFrom<u64>>::Error> {
        match value {
            0 => Ok(Verbosity::Error),
            1 => Ok(Verbosity::Warn),
            2 => Ok(Verbosity::Notice),
            3 => Ok(Verbosity::Info),
            4 => Ok(Verbosity::Talkative),
            5 => Ok(Verbosity::Chatty),
            6 => Ok(Verbosity::Debug),
            7 => Ok(Verbosity::Vomit),
            _ => Err(()),
        }
    }
}

impl<'a> NixDeserialize<'a> for Verbosity {
    fn deserialize(
        _protocol_version: super::ProtocolVersion,
        input: &[u8],
    ) -> nom::IResult<&[u8], Self> {
        let (input, verbosity) = nom::number::le_u64().parse(input)?;

        Ok((input, Self::try_from(verbosity).unwrap()))
    }
}
