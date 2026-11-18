use crate::wire::{NixDeserialize, NixSerialize, StorePath, UnkeyedValidPathInfo};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidPathInfo {
    path: StorePath,
    info: UnkeyedValidPathInfo,
}

impl ValidPathInfo {
    pub fn new(path: StorePath, info: UnkeyedValidPathInfo) -> Self {
        Self { path, info }
    }
}

impl<'a> NixDeserialize<'a> for ValidPathInfo {
    fn deserialize(
        protocol_version: super::ProtocolVersion,
        input: &'a [u8],
    ) -> nom::IResult<&'a [u8], Self> {
        let (input, path) = StorePath::deserialize(protocol_version, input)?;
        let (input, info) = UnkeyedValidPathInfo::deserialize(protocol_version, input)?;

        Ok((input, ValidPathInfo { path, info }))
    }
}

impl NixSerialize for ValidPathInfo {
    fn serialize(&self, protocol_version: super::ProtocolVersion, bytes: &mut bytes::BytesMut) {
        self.path.serialize(protocol_version, bytes);
        self.info.serialize(protocol_version, bytes);
    }
}
