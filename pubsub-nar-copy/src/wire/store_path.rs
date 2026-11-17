use std::fmt::Display;

use bytes::{BufMut as _, BytesMut};
use nom::{AsBytes, IResult, Parser as _, number::Endianness};

use crate::wire::{self, NixDeserialize, NixSerialize, ProtocolVersion};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorePath {
    pub store_dir: Option<String>,
    pub hash: [u8; 20],
    pub name: String,
}

impl TryFrom<&str> for StorePath {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let (string, store_dir) = if let Some(last_slash) = value.rfind('/') {
            // probably absolute
            let store_dir = value[0..last_slash].to_string();
            (&value[last_slash + 1..], Some(store_dir))
        } else {
            (value, None)
        };

        let first_dash = string.find('-').expect("a dash in the store path");
        let hash = base32::decode(base32::Alphabet::Crockford, &string[0..first_dash])
            .expect("a valid base32 encoded hash")
            .try_into()
            .expect("a hash of length 160 bits (20 bytes)");

        let name = string[first_dash + 1..].to_string();

        Ok(Self {
            store_dir,
            hash,
            name,
        })
    }
}

impl From<StorePath> for String {
    fn from(value: StorePath) -> Self {
        let encoded_hash = base32::encode(base32::Alphabet::Crockford, &value.hash).to_lowercase();

        if let Some(store_dir) = value.store_dir {
            format!("{}/{}-{}", store_dir, encoded_hash, value.name)
        } else {
            format!("{}-{}", encoded_hash, value.name)
        }
    }
}

impl Display for StorePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let encoded_hash = base32::encode(base32::Alphabet::Crockford, &self.hash).to_lowercase();

        if let Some(store_dir) = &self.store_dir {
            write!(f, "{}/{}-{}", store_dir, encoded_hash, self.name)
        } else {
            write!(f, "{}-{}", encoded_hash, self.name)
        }
    }
}

impl StorePath {
    pub fn into_absolute(mut self, store_dir: String) -> Self {
        self.store_dir = Some(store_dir);
        self
    }

    pub fn into_relative(mut self) -> Self {
        self.store_dir = None;
        self
    }
}

impl NixDeserialize for StorePath {
    fn deserialize(protocol_version: ProtocolVersion, input: &[u8]) -> IResult<&[u8], Self> {
        let (input, string) = String::deserialize(protocol_version, input)?;

        Ok((input, string.as_str().try_into().expect("no error")))
    }
}

impl NixSerialize for StorePath {
    fn serialize(&self, _protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        let mut size = 0;
        if let Some(store_dir) = &self.store_dir {
            // and if we have a store dir, then 1 for /
            size += store_dir.as_bytes().len() + 1;
        }

        let encoded_hash = base32::encode(base32::Alphabet::Crockford, &self.hash).to_lowercase();

        size += encoded_hash.as_bytes().len();
        size += 1 + self.name.as_bytes().len();

        let padding = wire::bytes::calculate_padding(size as u64);
        bytes.reserve(size_of::<u64>() + size + padding as usize);
        bytes.put_u64_le(size as u64);
        if let Some(store_dir) = &self.store_dir {
            bytes.put_slice(store_dir.as_bytes());
            bytes.put_slice(b"/");
        }
        bytes.put_slice(encoded_hash.as_bytes());
        bytes.put_slice(b"-");
        bytes.put_slice(self.name.as_bytes());
        for _ in 0..padding {
            bytes.put_u8(0);
        }
    }
}

#[cfg(test)]
mod test {
    use nom::AsBytes;

    use super::*;

    #[test]
    fn roundtrip_absolute() {
        let hash = "7fppvgqxs3zxhvrpqnmwydn70bzgbnnx";
        let string_store_path = format!("/nix/store/{}-rustc-wrapper-1.95.0", hash);

        let store_path = StorePath::try_from(string_store_path.as_str()).unwrap();
        assert_eq!(
            store_path,
            StorePath {
                store_dir: Some("/nix/store".to_string()),
                hash: base32::decode(base32::Alphabet::Crockford, hash)
                    .unwrap()
                    .try_into()
                    .unwrap(),
                name: "rustc-wrapper-1.95.0".to_string()
            }
        );

        let mut store_path_bytes = BytesMut::new();
        store_path.serialize(ProtocolVersion::default(), &mut store_path_bytes);

        let (remainder, store_path_roundtrip) =
            StorePath::deserialize(ProtocolVersion::default(), store_path_bytes.as_ref()).unwrap();

        assert!(
            remainder.is_empty(),
            "remainder isn't empty, it is {:?}",
            remainder
        );
        assert_eq!(store_path_roundtrip, store_path);
    }

    #[test]
    fn roundtrip_relative() {
        let hash = "7fppvgqxs3zxhvrpqnmwydn70bzgbnnx";
        let string_store_path = format!("{}-rustc-wrapper-1.95.0", hash);

        let store_path = StorePath::try_from(string_store_path.as_str()).unwrap();
        assert_eq!(
            store_path,
            StorePath {
                store_dir: None,
                hash: base32::decode(base32::Alphabet::Crockford, hash)
                    .unwrap()
                    .try_into()
                    .unwrap(),
                name: "rustc-wrapper-1.95.0".to_string()
            }
        );

        let mut store_path_bytes = BytesMut::new();
        store_path.serialize(ProtocolVersion::default(), &mut store_path_bytes);

        let (remainder, store_path_roundtrip) =
            StorePath::deserialize(ProtocolVersion::default(), store_path_bytes.as_ref()).unwrap();

        assert!(
            remainder.is_empty(),
            "remainder isn't empty, it is {:?}",
            remainder
        );
        assert_eq!(store_path_roundtrip, store_path);
    }
}
