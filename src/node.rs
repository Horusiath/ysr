use crate::block::ID;
use crate::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub type NodeID = U64;

#[repr(C)]
#[derive(FromBytes, KnownLayout, Immutable, IntoBytes)]
pub(crate) struct NodeHeader {
    start: ID,
}
