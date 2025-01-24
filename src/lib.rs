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

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type U16 = zerocopy::U16<zerocopy::byteorder::LE>;
pub type U32 = zerocopy::U32<zerocopy::byteorder::LE>;
pub type U64 = zerocopy::U64<zerocopy::byteorder::LE>;
pub type U128 = zerocopy::U128<zerocopy::byteorder::LE>;
pub type ClientID = U64;
pub type Clock = U64;

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
    #[error("failed to map data ")]
    InvalidMapping(&'static str),
    #[error("malformed block: {0}")]
    MalformedBlock(ID),
    #[error("unsupported content type: {0}")]
    UnsupportedContent(u8),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid lib0 data: {0}")]
    Lib0(#[from] Box<crate::lib0::Error>),
    #[error("store error: {0}")]
    Store(DynError),
}
