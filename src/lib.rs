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
mod varint;
mod write;

use crate::block::ID;
pub use multi_doc::MultiDoc;
pub use state_vector::StateVector;
pub use store::Store;
pub use transaction::Transaction;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type U16 = zerocopy::U16<zerocopy::byteorder::LE>;
pub type U32 = zerocopy::U32<zerocopy::byteorder::LE>;
pub type U64 = zerocopy::U64<zerocopy::byteorder::LE>;
pub type U128 = zerocopy::U128<zerocopy::byteorder::LE>;
pub type Clock = U32;

pub type DynError = Box<dyn std::error::Error + Send + Sync>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("expected more data, reached end of buffer")]
    EndOfBuffer,
    #[error("value is out of range of expected type")]
    ValueOutOfRange,
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
    #[error("store error: {0}")]
    Store(DynError),
    #[error("block not found: {0}")]
    BlockNotFound(ID),
    #[error("Client ID is not valid 53-bit integer")]
    ClientIDOutOfRange,
    #[error("LMDB error: {0}")]
    Lmdb(#[from] lmdb_rs_m::MdbError),
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
    const MAX_VALUE: Self = ClientID(U32::new((1u32 << 31) - 1));

    pub fn new_random() -> Self {
        let value: u32 = rand::random_range(..((1u32 << 31) - 1));
        Self(value.into())
    }

    pub fn is_valid(self) -> bool {
        self <= Self::MAX_VALUE
    }

    pub fn new(id: U32) -> Option<Self> {
        let id = Self(id.into());
        if id.is_valid() {
            Some(id)
        } else {
            None
        }
    }

    #[inline]
    pub const unsafe fn new_unchecked(id: u32) -> Self {
        Self(U32::new(id))
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
