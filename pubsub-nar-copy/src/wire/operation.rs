use bytes::{BufMut as _, BytesMut};
use nom::{IResult, Parser as _, number::Endianness};

use crate::wire::{NixDeserialize, NixSerialize, ProtocolVersion};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Operation {
    IsValidPath = 1,
    QueryReferrers = 6,
    AddToStore = 7,
    BuildPaths = 9,
    EnsurePath = 10,
    AddTempRoot = 11,
    AddIndirectRoot = 12,
    FindRoots = 14,
    SetOptions = 19,
    CollectGarbage = 20,
    QueryAllValidPaths = 23,
    QueryPathInfo = 26,
    QueryPathFromHashPart = 29,
    QueryValidPaths = 31,
    QuerySubstitutablePaths = 32,
    QueryValidDerivers = 33,
    OptimiseStore = 34,
    VerifyStore = 35,
    BuildDerivation = 36,
    AddSignatures = 37,
    NarFromPath = 38,
    AddToStoreNar = 39,
    QueryMissing = 40,
    QueryDerivationOutputMap = 41,
    RegisterDrvOutput = 42,
    QueryRealisation = 43,
    AddMultipleToStore = 44,
    AddBuildLog = 45,
    BuildPathsWithResults = 46,
    AddPermRoot = 47,
}

impl TryFrom<u64> for Operation {
    type Error = String;

    // \([^ ]+\) = \([[:digit:]]+\),$ → \2 => Ok(Operation::\1),
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Operation::IsValidPath),
            6 => Ok(Operation::QueryReferrers),
            7 => Ok(Operation::AddToStore),
            9 => Ok(Operation::BuildPaths),
            10 => Ok(Operation::EnsurePath),
            11 => Ok(Operation::AddTempRoot),
            12 => Ok(Operation::AddIndirectRoot),
            14 => Ok(Operation::FindRoots),
            19 => Ok(Operation::SetOptions),
            20 => Ok(Operation::CollectGarbage),
            23 => Ok(Operation::QueryAllValidPaths),
            26 => Ok(Operation::QueryPathInfo),
            29 => Ok(Operation::QueryPathFromHashPart),
            31 => Ok(Operation::QueryValidPaths),
            32 => Ok(Operation::QuerySubstitutablePaths),
            33 => Ok(Operation::QueryValidDerivers),
            34 => Ok(Operation::OptimiseStore),
            35 => Ok(Operation::VerifyStore),
            36 => Ok(Operation::BuildDerivation),
            37 => Ok(Operation::AddSignatures),
            38 => Ok(Operation::NarFromPath),
            39 => Ok(Operation::AddToStoreNar),
            40 => Ok(Operation::QueryMissing),
            41 => Ok(Operation::QueryDerivationOutputMap),
            42 => Ok(Operation::RegisterDrvOutput),
            43 => Ok(Operation::QueryRealisation),
            44 => Ok(Operation::AddMultipleToStore),
            45 => Ok(Operation::AddBuildLog),
            46 => Ok(Operation::BuildPathsWithResults),
            47 => Ok(Operation::AddPermRoot),
            _ => Err(format!("invalid operation number {}", value)),
        }
    }
}

impl From<&Operation> for u64 {
    fn from(value: &Operation) -> Self {
        match value {
            Operation::IsValidPath => 1,
            Operation::QueryReferrers => 6,
            Operation::AddToStore => 7,
            Operation::BuildPaths => 9,
            Operation::EnsurePath => 10,
            Operation::AddTempRoot => 11,
            Operation::AddIndirectRoot => 12,
            Operation::FindRoots => 14,
            Operation::SetOptions => 19,
            Operation::CollectGarbage => 20,
            Operation::QueryAllValidPaths => 23,
            Operation::QueryPathInfo => 26,
            Operation::QueryPathFromHashPart => 29,
            Operation::QueryValidPaths => 31,
            Operation::QuerySubstitutablePaths => 32,
            Operation::QueryValidDerivers => 33,
            Operation::OptimiseStore => 34,
            Operation::VerifyStore => 35,
            Operation::BuildDerivation => 36,
            Operation::AddSignatures => 37,
            Operation::NarFromPath => 38,
            Operation::AddToStoreNar => 39,
            Operation::QueryMissing => 40,
            Operation::QueryDerivationOutputMap => 41,
            Operation::RegisterDrvOutput => 42,
            Operation::QueryRealisation => 43,
            Operation::AddMultipleToStore => 44,
            Operation::AddBuildLog => 45,
            Operation::BuildPathsWithResults => 46,
            Operation::AddPermRoot => 47,
        }
    }
}

impl From<Operation> for u64 {
    fn from(value: Operation) -> Self {
        (&value).into()
    }
}

impl<'a> NixDeserialize<'a> for Operation {
    fn deserialize(_protocol_version: ProtocolVersion, input: &[u8]) -> IResult<&[u8], Self> {
        let (input, operation) = nom::number::u64(Endianness::Little).parse(input)?;

        Ok((input, TryFrom::try_from(operation).unwrap()))
    }
}

impl NixSerialize for Operation {
    fn serialize(&self, _protocol_version: ProtocolVersion, bytes: &mut BytesMut) {
        bytes.put_u64_le(self.into())
    }
}
