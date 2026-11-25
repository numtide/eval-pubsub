use std::fmt::{Display, Write};

use bytes::{BufMut as _, BytesMut};
use itertools::Itertools;
use nix_base32::{from_nix_base32, to_nix_base32};
use nom::{AsBytes, IResult, Parser as _};

use crate::wire::{
    self, NixDeserialize, NixSerialize, ProtocolVersion, StorePathHash, protocol_version,
};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct StorePath {
    pub store_dir: Option<String>,
    pub hash: StorePathHash,
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
        let hash = from_nix_base32(&string[0..first_dash])
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

impl TryFrom<String> for StorePath {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl From<&StorePath> for String {
    fn from(value: &StorePath) -> Self {
        let encoded_hash: String = (&value.hash).into();

        if let Some(store_dir) = &value.store_dir {
            format!("{}/{}-{}", store_dir, encoded_hash, value.name)
        } else {
            format!("{}-{}", encoded_hash, value.name)
        }
    }
}

impl std::fmt::Debug for StorePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_string().as_str())
    }
}

impl Display for StorePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let encoded_hash: String = (&self.hash).into();

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

fn parse_string<'a>(input: &'a [u8]) -> IResult<&'a [u8], String> {
    nom::multi::fold(
        0..,
        nom::character::complete::anychar,
        String::new,
        |mut string, c| {
            string.push(c);
            string
        },
    )
    .parse(input)
}

impl<'a> NixDeserialize<'a> for StorePath {
    fn deserialize(_protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        wire::bytes::with_bytes(
            |input: &'a [u8]| {
                let (store_dir, path) = if nom::character::anychar(input)?.1 == '/' {
                    let (input, parts): (_, Vec<&'a [u8]>) = nom::multi::separated_list1(
                        nom::character::complete::char('/'),
                        nom::bytes::complete::take_till(|c| c == '/' as u8),
                    )
                    .parse(&input[1..])?;
                    let _ = nom::combinator::eof(input)?;

                    let (path, store_dir) = parts.split_last().unwrap();
                    let store_dir: String = ["/"]
                        .into_iter()
                        .chain(
                            store_dir
                                .into_iter()
                                .map(|b| unsafe { str::from_utf8_unchecked(b) })
                                .intersperse("/"),
                        )
                        .collect();

                    (Some(store_dir), *path)
                } else {
                    (None, input)
                };

                println!("got input {:?}", path);

                let (input, (hash, name)) = nom::sequence::separated_pair(
                    nom::bytes::complete::is_a("0123456789abcdfghijklmnpqrsvwxyz"),
                    nom::character::char('-'),
                    parse_string,
                )
                .parse(path)?;

                println!("got hash {:?} and name {}", hash, name);

                let hash: [u8; 20] = from_nix_base32(unsafe { str::from_utf8_unchecked(hash) })
                    .expect("a valid base32 encoded hash")
                    .try_into()
                    .expect("a hash of length 160 bits (20 bytes)");

                println!("got hash {:?} and name {} with {:?}", hash, name, input);

                Ok((
                    input,
                    Self {
                        store_dir: store_dir,
                        hash: StorePathHash { hash },
                        name,
                    },
                ))
            },
            true,
        )
        .parse(input)
    }
}

impl NixSerialize for StorePath {
    fn serialize(&self, _protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        let mut size = 0;
        if let Some(store_dir) = &self.store_dir {
            // and if we have a store dir, then 1 for /
            size += store_dir.as_bytes().len() + 1;
        }

        let encoded_hash: String = (&self.hash).into();

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
                hash: from_nix_base32(hash).unwrap().try_into().unwrap(),
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
        let name = "rustc-wrapper-1.95.0";
        let string_store_path = format!("{}-{}", hash, name);

        let store_path = StorePath::try_from(string_store_path.as_str()).unwrap();
        assert_eq!(
            store_path,
            StorePath {
                store_dir: None,
                hash: from_nix_base32(hash).unwrap().try_into().unwrap(),
                name: name.to_owned()
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
