use nom::IResult;

use crate::wire::{NixDeserialize, ProtocolVersion};

pub struct Nar {
    pub bytes: Vec<u8>,
}

impl<'a> NixDeserialize<'a> for Nar {
    fn deserialize(protocol_version: ProtocolVersion, input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let orig_input = input;
        let (input, header) = String::deserialize(protocol_version, input)?;
        assert_eq!(header, "nix-archive-1");
        let (input, _nix_obj) = deserialize_nar_obj(protocol_version, input)?;

        Ok((
            input,
            Nar {
                bytes: Vec::from(&orig_input[0..orig_input.len() - input.len()]),
            },
        ))
    }
}

fn deserialize_regular<'a>(
    protocol_version: ProtocolVersion,
    input: &'a [u8],
) -> IResult<&'a [u8], ()> {
    let (input, tag) = String::deserialize(protocol_version, input)?;
    let input = if tag == "executable" {
        String::deserialize(protocol_version, input)?.0
    } else if tag == "content" {
        input
    } else {
        panic!("uknown regular file field")
    };

    let (input, tag) = String::deserialize(protocol_version, input)?;
    assert_eq!(tag, "contents");

    let (input, _content) = String::deserialize(protocol_version, input)?;

    Ok((input, ()))
}

fn deserialize_symlink<'a>(
    protocol_version: ProtocolVersion,
    input: &'a [u8],
) -> IResult<&'a [u8], ()> {
    let (input, tag) = String::deserialize(protocol_version, input)?;
    assert_eq!(tag, "target");
    let (input, _target) = String::deserialize(protocol_version, input)?;

    Ok((input, ()))
}

fn deserialize_directory<'a>(
    protocol_version: ProtocolVersion,
    input: &'a [u8],
) -> IResult<&'a [u8], ()> {
    let mut input_orig = input;
    loop {
        let (input, tag) = String::deserialize(protocol_version, input_orig)?;

        if tag == "entry" {
            let (input, _directory_entry) = deserialize_directory_entry(protocol_version, input)?;
            input_orig = input;
        } else {
            break;
        }
    }

    Ok((input_orig, ()))
}

fn deserialize_directory_entry<'a>(
    protocol_version: ProtocolVersion,
    input: &'a [u8],
) -> IResult<&'a [u8], ()> {
    let (input, lparen) = String::deserialize(protocol_version, input)?;
    assert_eq!(lparen, "(");

    let (input, tag) = String::deserialize(protocol_version, input)?;
    assert_eq!(tag, "name");

    let (input, _name) = String::deserialize(protocol_version, input)?;

    let (input, tag) = String::deserialize(protocol_version, input)?;
    assert_eq!(tag, "node");

    let (input, _nar_obj) = deserialize_nar_obj(protocol_version, input)?;

    let (input, rparen) = String::deserialize(protocol_version, input)?;
    assert_eq!(rparen, ")");

    Ok((input, ()))
}

fn deserialize_nar_obj_inner<'a>(
    protocol_version: ProtocolVersion,
    input: &'a [u8],
) -> IResult<&'a [u8], ()> {
    let (input, tag) = String::deserialize(protocol_version, input)?;
    assert_eq!(tag, "type");

    let (input, r#type) = String::deserialize(protocol_version, input)?;
    match r#type.as_str() {
        "regular" => deserialize_regular(protocol_version, input),
        "symlink" => deserialize_symlink(protocol_version, input),
        "directory" => deserialize_directory(protocol_version, input),
        _ => panic!("invalid type"),
    }
}

fn deserialize_nar_obj<'a>(
    protocol_version: ProtocolVersion,
    input: &'a [u8],
) -> IResult<&'a [u8], ()> {
    let (input, lparen) = String::deserialize(protocol_version, input)?;
    assert_eq!(lparen, "(");

    let (input, _nar_obj) = deserialize_nar_obj_inner(protocol_version, input)?;

    let (input, rparen) = String::deserialize(protocol_version, input)?;
    assert_eq!(rparen, ")");

    Ok((input, ()))
}
