use bytes::{BufMut, Bytes, BytesMut};
use nom::IResult;
use std::io::{Cursor, Read};

use crate::wire::{self, NixDeserialize, NixSerialize, ProtocolVersion};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Signature {
    name: String,
    signature: Bytes,
}

impl From<&Signature> for String {
    fn from(value: &Signature) -> Self {
        let signature = std::io::Cursor::new(Vec::new());
        let signature = base64::write::EncoderWriter::new(
            signature,
            &base64::engine::general_purpose::STANDARD,
        )
        .finish()
        .unwrap();

        let signature = unsafe { String::from_utf8_unchecked(signature.into_inner()) };
        format!("{}:{}", value.name, signature)
    }
}

impl TryFrom<&str> for Signature {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut value = value.split(':');

        if let Some(name) = value.next() {
            if let Some(sig) = value.next() {
                if let None = value.next() {
                    let sig = Cursor::new(sig);
                    let mut decoder = base64::read::DecoderReader::new(
                        sig,
                        &base64::engine::general_purpose::STANDARD,
                    );
                    let sig = BytesMut::new();
                    let mut writer = sig.writer();
                    std::io::copy(&mut decoder, &mut writer)
                        .map_err(|_| "IO copy error".to_owned())?;
                    let sig = writer.into_inner();

                    return Ok(Self {
                        name: name.to_owned(),
                        signature: sig.freeze(),
                    });
                }
            }
        }

        return Err("signature didn't have exactly two ':' delimited segments".to_owned());
    }
}

impl TryFrom<String> for Signature {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(TryFrom::try_from(value.as_str())?)
    }
}

impl Signature {
    pub fn new<S>(name: S, signature: Bytes) -> Signature
    where
        S: Into<String>,
    {
        Self {
            name: name.into(),
            signature: signature.into(),
        }
    }

    pub fn new_from_base64<S, B>(name: S, signature: B) -> Signature
    where
        S: Into<String>,
        B: AsRef<[u8]>,
    {
        let mut reader = Cursor::new(signature);
        let mut decoder = base64::read::DecoderReader::new(
            &mut reader,
            &base64::engine::general_purpose::STANDARD,
        );

        let bytes = BytesMut::new();
        let mut writer = bytes.writer();
        std::io::copy(&mut decoder, &mut writer).unwrap();
        let bytes = writer.into_inner();

        Signature::new(name, bytes.freeze())
    }
}

impl<'a> NixDeserialize<'a> for Signature {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, string) = String::deserialize(protocol_version, input)?;

        Ok((
            input,
            Self::try_from(string.as_str()).expect("invalid signature"),
        ))
    }
}
impl NixSerialize for Signature {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        let mut output = BytesMut::new();
        output.put_slice(self.name.as_bytes());
        output.put_slice(b":");
        let output = output.writer();
        let mut encoder =
            base64::write::EncoderWriter::new(output, &base64::engine::general_purpose::STANDARD);
        std::io::copy(&mut Cursor::new(&self.signature), &mut encoder).unwrap();
        let output = encoder.finish().unwrap();

        &output.into_inner().serialize(protocol_version, bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let signature = Signature::new_from_base64(
            "buildbot.in.redalder.org",
            b"MahFtP9vE7Mu9mD4aJrbgQN8XQMC5aJOZhUj32/TZJ72FunPAjPiuSBzdAJ+u1OARL7wfkdiLuM82/rxU3o8CA==",
        );

        let mut signature_bytes = BytesMut::new();

        signature.serialize(ProtocolVersion::default(), &mut signature_bytes);

        let (remainder, signature_roundtrip) =
            Signature::deserialize(ProtocolVersion::default(), &signature_bytes).unwrap();

        assert!(
            remainder.is_empty(),
            "remainder isn't empty, it is {:?}",
            remainder
        );
        assert_eq!(signature, signature_roundtrip);
    }
}
