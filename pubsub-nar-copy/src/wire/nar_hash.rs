use std::io::Cursor;

use bytes::{BufMut as _, BytesMut};
use nom::IResult;

use crate::wire::{self, NixDeserialize, NixSerialize, ProtocolVersion};

#[derive(PartialEq, Copy, Clone, Debug, Eq)]
pub struct NarHash([u8; 32]);

impl NarHash {
    pub fn new_from_base64<B>(hash: B) -> Self
    where
        B: AsRef<str>,
    {
        let mut decoder = base64::read::DecoderReader::new(
            Cursor::new(hash.as_ref()),
            &base64::engine::general_purpose::STANDARD,
        );
        let mut hash = BytesMut::with_capacity(32).writer();
        std::io::copy(&mut decoder, &mut hash).unwrap();
        let hash = hash.into_inner();

        println!("nar hash length {}", hash.len());

        Self(hash.as_ref().try_into().unwrap())
    }
}

impl<'a> NixDeserialize<'a> for NarHash {
    fn deserialize(
        protocol_version: ProtocolVersion,
        input: &'a [u8],
    ) -> IResult<&'a [u8], NarHash> {
        let (input, string) = String::deserialize(protocol_version, input)?;

        let hash = base16::decode(&string).unwrap();

        Ok((input, NarHash(hash.try_into().unwrap())))
    }
}

impl NixSerialize for NarHash {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        let hash = base16::encode_lower(&self.0);

        hash.serialize(protocol_version, bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let nar_hash = NarHash::new_from_base64("AmhLoypwXno60vijTiT5CzovprGfQuHb4RyjjUY5seA=");

        let mut nar_hash_bytes = BytesMut::new();

        nar_hash.serialize(ProtocolVersion::default(), &mut nar_hash_bytes);

        let (remainder, nar_hash_roundtrip) =
            NarHash::deserialize(ProtocolVersion::default(), nar_hash_bytes.as_ref()).unwrap();

        assert!(
            remainder.is_empty(),
            "remainder isn't empty, it is {:?}",
            remainder
        );
        assert_eq!(nar_hash_roundtrip, nar_hash);
    }
}
