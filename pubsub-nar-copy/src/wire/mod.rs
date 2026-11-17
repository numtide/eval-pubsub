mod bytes;
mod error;
mod nar_hash;
mod operation;
mod opt_store_path;
mod protocol_version;
mod set;
mod signature;
mod size;
mod store_path;
mod string;
mod time;
mod trace_line;
mod trust;
mod unkeyed_valid_path_info;
mod verbosity;

use ::bytes::BytesMut;
use nom::IResult;

pub use error::*;
pub use nar_hash::*;
pub use operation::*;
pub use opt_store_path::*;
pub use protocol_version::*;
pub use signature::*;
pub use size::*;
pub use store_path::*;
pub use time::*;
pub use trace_line::*;
pub use trust::*;
pub use unkeyed_valid_path_info::*;
pub use verbosity::*;

pub trait NixDeserialize
where
    Self: Sized,
{
    fn deserialize(protocol_version: ProtocolVersion, input: &[u8]) -> IResult<&[u8], Self>;
}

pub trait NixSerialize
where
    Self: Sized,
{
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut);
}
