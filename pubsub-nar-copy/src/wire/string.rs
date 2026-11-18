use bytes::BytesMut;
use nom::IResult;

use crate::{
    ProtocolVersion,
    wire::{self, NixDeserialize, NixSerialize},
};

impl<'a> NixDeserialize<'a> for String {
    fn deserialize(
        protocol_version: ProtocolVersion,
        input: &'a [u8],
    ) -> IResult<&'a [u8], String> {
        let (input, bytes) = <Vec<u8>>::deserialize(protocol_version, input)?;

        Ok((input, String::from_utf8(bytes).unwrap()))
    }
}

impl NixSerialize for &str {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        self.as_bytes().serialize(protocol_version, bytes);
    }
}

impl NixSerialize for &String {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        self.as_str().serialize(protocol_version, bytes);
    }
}

impl NixSerialize for String {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        self.as_str().serialize(protocol_version, bytes);
    }
}
