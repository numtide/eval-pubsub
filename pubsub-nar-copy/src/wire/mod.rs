pub mod bytes;
mod error;
mod nar;
mod nar_hash;
mod operation;
mod opt_store_path;
mod protocol_version;
mod set;
mod signature;
mod size;
mod store_path;
mod store_path_hash;
mod string;
mod time;
mod trace_line;
mod trust;
mod unkeyed_valid_path_info;
mod valid_path_info;
mod verbosity;

use ::bytes::{BufMut, BytesMut};
use nom::{IResult, Parser as _};

pub use error::*;
pub use nar::*;
pub use nar_hash::*;
pub use operation::*;
pub use opt_store_path::*;
pub use protocol_version::*;
pub use signature::*;
pub use size::*;
pub use store_path::*;
pub use store_path_hash::*;
pub use time::*;
pub use trace_line::*;
pub use trust::*;
pub use unkeyed_valid_path_info::*;
pub use valid_path_info::*;
pub use verbosity::*;

pub trait NixDeserialize<'a>
where
    Self: Sized,
{
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self>;
}

pub trait NixSerialize
where
    Self: Sized,
{
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut);
}

impl NixDeserialize<'_> for bool {
    fn deserialize(_protocol_version: ProtocolVersion, input: &'_ [u8]) -> IResult<&'_ [u8], Self> {
        let (input, ultimate) = nom::number::le_u64().parse(input)?;
        Ok((input, ultimate > 0))
    }
}

impl NixSerialize for bool {
    fn serialize(&self, _protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        bytes.put_u64_le(if *self { 1 } else { 0 })
    }
}

impl NixDeserialize<'_> for u64 {
    fn deserialize(_protocol_version: ProtocolVersion, input: &'_ [u8]) -> IResult<&'_ [u8], Self> {
        let (input, value) = nom::number::le_u64().parse(input)?;
        Ok((input, value))
    }
}

impl NixSerialize for u64 {
    fn serialize(&self, _protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        bytes.put_u64_le(*self)
    }
}
