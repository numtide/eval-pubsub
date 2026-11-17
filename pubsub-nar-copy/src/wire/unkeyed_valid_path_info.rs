use bytes::BufMut;
use bytes::BytesMut;
use nom::IResult;
use nom::Parser as _;

use crate::wire;
use crate::wire::NarHash;
use crate::wire::NixDeserialize;
use crate::wire::NixSerialize;
use crate::wire::OptStorePath;
use crate::wire::ProtocolVersion;
use crate::wire::Signature;
use crate::wire::StorePath;
use crate::wire::Time;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnkeyedValidPathInfo {
    deriver: OptStorePath,
    nar_hash: NarHash,
    references: Vec<StorePath>,
    registration_time: Time,
    nar_size: u64,
    ultimate: Option<bool>,
    signatures: Option<Vec<Signature>>,
    ca: Option<String>,
}

impl NixDeserialize for UnkeyedValidPathInfo {
    fn deserialize(
        protocol_version: ProtocolVersion,
        input: &[u8],
    ) -> IResult<&[u8], UnkeyedValidPathInfo> {
        let (input, deriver) = OptStorePath::deserialize(protocol_version, input)?;
        let (input, nar_hash) = NarHash::deserialize(protocol_version, input)?;
        let (input, references) =
            <Vec<StorePath> as NixDeserialize>::deserialize(protocol_version, input)?;
        let (input, registration_time) = Time::deserialize(protocol_version, input)?;
        let (input, nar_size) = nom::number::le_u64().parse(input)?;

        let (input, (ultimate, signatures, ca)) =
            if protocol_version >= ProtocolVersion::from_parts(1, 16) {
                let (input, ultimate) = nom::number::le_u64().parse(input)?;
                let ultimate = ultimate > 0;
                let (input, signatures) =
                    <Vec<Signature> as NixDeserialize>::deserialize(protocol_version, input)?;
                let (input, ca) = String::deserialize(protocol_version, input)?;

                (
                    input,
                    (
                        Some(ultimate),
                        Some(signatures),
                        if ca.is_empty() { None } else { Some(ca) },
                    ),
                )
            } else {
                (input, (None, None, None))
            };
        Ok((
            input,
            Self {
                deriver,
                nar_hash,
                references,
                registration_time,
                nar_size,
                ultimate,
                signatures,
                ca,
            },
        ))
    }
}

impl NixSerialize for UnkeyedValidPathInfo {
    fn serialize(&self, protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        self.deriver.serialize(protocol_version, bytes);
        self.nar_hash.serialize(protocol_version, bytes);
        self.references.serialize(protocol_version, bytes);
        self.registration_time.serialize(protocol_version, bytes);
        bytes.put_u64_le(self.nar_size);
        if protocol_version >= ProtocolVersion::from_parts(1, 16) {
            bytes.put_u64_le(if self.ultimate.expect("valid ultimate") {
                1
            } else {
                0
            });

            self.signatures
                .as_ref()
                .expect("valid signatures")
                .serialize(protocol_version, bytes);
            match &self.ca {
                Some(ca) => ca.serialize(protocol_version, bytes),
                None => "".serialize(protocol_version, bytes),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::wire::OptStorePath;

    #[test]
    fn roundtrip() {
        let deriver =
            OptStorePath::try_from("/nix/store/8dgk8gnqwsp1yy9zsdb1k1hrh2sq24z9-curl-8.22.0.drv")
                .unwrap();
        let path_info = UnkeyedValidPathInfo {
            deriver,
            nar_hash: NarHash::new_from_base64("AmhLoypwXno60vijTiT5CzovprGfQuHb4RyjjUY5seA="),
            references: vec![
                "/nix/store/10dxp0qxqxxsyiljrh2kp0xqhz6arhcx-bash-5.3p15",
                "/nix/store/36alc2mwna4nzf6djdj2hvh4w4r3cai5-curl-8.22.0",
                "/nix/store/6vqggv3cdqkh098nm8brbs2mwhn6bciy-libssh2-1.11.1",
                "/nix/store/7fjvn38n3dnv092jbmb4m87ik5qvmj7g-brotli-1.2.0-lib",
                "/nix/store/80z9njiswl9xm6qk93zsjr5bj8v0z2lm-krb5-1.22.2-lib",
                "/nix/store/8vq8ivlhfqkbhs7r9mmirsdp8gh8hmrm-ngtcp2-1.23.0",
                "/nix/store/cxd8d2vg70c3pa31v2473hphaa1v0j44-openssl-3.5.8",
                "/nix/store/h4wfwic161kxrr74jlzla5lsm28hgary-glibc-2.44-25",
                "/nix/store/lhk23vackrdgmxya733h3wp01xz622gn-libpsl-0.21.5",
                "/nix/store/ph4ib8mxy5yrw40k7a1l03bfd58hq6wj-zstd-1.5.7",
                "/nix/store/q0lbdx5yv93a3md74kjhm0v2kljjii89-libidn2-2.3.8",
                "/nix/store/r9n1wja9zhj3w4r71pggxal3c6xqak2a-nghttp3-1.17.0",
                "/nix/store/sky8mslz50n51np0z4vhdq61jba12fh2-nghttp2-1.70.0-lib",
            ]
            .into_iter()
            .map(StorePath::try_from)
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
            registration_time: Time::new(1792259102),
            nar_size: 387528,
            ultimate: Some(false),
            signatures: Some(vec![
                "cache.nixos.org-1:7quQ3efvQruDNsI0EVCN5zuy/ubbEi3Z6xq+sIIze3g5/52GZackQW702X/d/InsGQd4xa+/WR0jreTa4crJBQ==",
            ].into_iter().map(Signature::try_from).collect::<Result<Vec<_>, _>>().unwrap()),
            ca: None,
        };

        let mut path_info_bytes = BytesMut::new();
        path_info.serialize(ProtocolVersion::default(), &mut path_info_bytes);

        let (remainder, path_info_roundtrip) =
            UnkeyedValidPathInfo::deserialize(ProtocolVersion::default(), path_info_bytes.as_ref())
                .unwrap();

        assert!(
            remainder.is_empty(),
            "remainder isn't empty, it is {:?}",
            remainder
        );
        assert_eq!(path_info_roundtrip, path_info);
    }
}
