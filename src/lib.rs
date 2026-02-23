mod block;
mod block_reader;
mod content;
mod id_set;
pub mod lib0;
mod multi_doc;
mod node;
mod read;
mod state_vector;
mod store;
mod transaction;
mod types;
mod varint;
mod write;

mod block_cursor;
//mod bucket;
mod input;
mod integrate;
mod output;
mod prelim;
#[cfg(test)]
mod test_util;
mod update;

pub use crate::block::{Block, BlockHeader, BlockMut, ID};
pub use input::In;
use lmdb_rs_m::MdbError;
pub use multi_doc::MultiDoc;
pub use output::Out;
pub use read::DecoderV1;
use serde::{Deserialize, Serialize};
use smallvec::CollectionAllocErr;
pub use state_vector::StateVector;
use std::collections::TryReserveError;
pub use transaction::Transaction;
pub use types::list::{List, ListPrelim, ListRef};
pub use types::map::{Map, MapPrelim, MapRef};
pub use types::text::{Text, TextRef};
pub use types::{Mounted, Unmounted};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type U16 = zerocopy::U16<zerocopy::byteorder::BE>;
pub type U32 = zerocopy::U32<zerocopy::byteorder::BE>;
pub type U64 = zerocopy::U64<zerocopy::byteorder::BE>;
pub type U128 = zerocopy::U128<zerocopy::byteorder::BE>;
pub type Clock = U32;

pub type DynError = Box<dyn std::error::Error + Send + Sync>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("value under provided index or key was not found")]
    NotFound,
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("expected more data, reached end of buffer")]
    EndOfBuffer,
    #[error("operation tried to allocate too much memory")]
    OutOfMemory,
    #[error("index is out of range of expected type")]
    OutOfRange,
    #[error("provided key is longer than 255 bytes")]
    KeyTooLong,
    #[error("failed to map data to {0}")]
    InvalidMapping(&'static str),
    #[error("malformed block: {0}")]
    MalformedBlock(ID),
    #[error("unsupported content type: {0}")]
    UnsupportedContent(u8),
    #[error("unknown yjs collection type: {0}")]
    UnknownNodeType(u8),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid lib0 data: {0}")]
    Lib0(#[from] Box<crate::lib0::Error>),
    #[error("{0}")]
    Custom(DynError),
    #[error("block not found: {0}")]
    BlockNotFound(ID),
    #[error("Client ID is not valid 53-bit integer")]
    ClientIDOutOfRange,
    #[error("LMDB error: {0}")]
    Lmdb(#[from] lmdb_rs_m::MdbError),
    #[error("expected value couldn't be fit into containing data")]
    ValueTooLarge,
}

impl From<TryReserveError> for Error {
    fn from(_: TryReserveError) -> Self {
        Self::OutOfMemory
    }
}

impl From<CollectionAllocErr> for Error {
    fn from(_: CollectionAllocErr) -> Self {
        Self::OutOfMemory
    }
}

impl Into<std::fmt::Error> for Error {
    fn into(self) -> std::fmt::Error {
        std::fmt::Error
    }
}

impl Error {
    pub fn not_found(&self) -> bool {
        matches!(self, Error::NotFound)
    }
}

trait Optional {
    type Return;
    fn optional(self) -> Self::Return;
}

impl<T> Optional for Result<T, Error> {
    type Return = Result<Option<T>, Error>;

    fn optional(self) -> Self::Return {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(Error::NotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl<T> Optional for Result<T, MdbError> {
    type Return = Result<Option<T>, MdbError>;

    fn optional(self) -> Self::Return {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(MdbError::NotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[repr(transparent)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    Default,
    FromBytes,
    KnownLayout,
    Immutable,
    IntoBytes,
)]
pub struct ClientID(U32);

impl ClientID {
    const ROOT: Self = ClientID(U32::new(0));

    pub fn new_random() -> Self {
        let value: u32 = rand::random_range(..u32::MAX) + 1;
        Self(value.into())
    }

    pub fn is_valid(self) -> bool {
        self > Self::ROOT
    }

    pub fn new(id: U32) -> Option<Self> {
        let id = Self(id.into());
        if id.is_valid() { Some(id) } else { None }
    }

    #[inline]
    pub const unsafe fn new_unchecked(id: u32) -> Self {
        Self(U32::new(id))
    }

    pub fn parse(bytes: &[u8]) -> crate::Result<&Self> {
        Self::ref_from_bytes(bytes).map_err(|_| Error::InvalidMapping("ClientID"))
    }
}

impl std::fmt::Display for ClientID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:08x}", self.0.get())
    }
}

impl From<ClientID> for u32 {
    fn from(value: ClientID) -> Self {
        value.0.get()
    }
}

impl From<ClientID> for U32 {
    fn from(value: ClientID) -> Self {
        value.0
    }
}

impl From<u32> for ClientID {
    fn from(value: u32) -> Self {
        Self(U32::new(value))
    }
}

impl TryFrom<U32> for ClientID {
    type Error = crate::Error;

    fn try_from(value: U32) -> crate::Result<Self> {
        match Self::new(value) {
            None => Err(crate::Error::ClientIDOutOfRange),
            Some(id) => Ok(id),
        }
    }
}

impl Serialize for ClientID {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u32(self.0.get())
    }
}

impl<'de> Deserialize<'de> for ClientID {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        ClientID::try_from(value).map_err(serde::de::Error::custom)
    }
}
