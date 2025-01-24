use crate::block::ID;
use crate::{ClientID, U64};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout};

pub type NodeID = ID;

#[repr(C)]
#[derive(FromBytes, KnownLayout, Immutable, IntoBytes)]
pub(crate) struct NodeHeader {
    type_ref: u8,
    flags: NodeFlags,
    start: ID,
}

impl NodeHeader {
    pub fn new(type_ref: u8) -> Self {
        Self {
            type_ref,
            flags: NodeFlags(0),
            start: ID::new_zeroed(),
        }
    }
}

#[repr(transparent)]
#[derive(FromBytes, KnownLayout, Immutable, IntoBytes)]
pub(crate) struct NodeFlags(u8);

impl NodeFlags {}

impl NodeID {
    pub fn from_root(root: &[u8]) -> NodeID {
        let hash = twox_hash::XxHash64::oneshot(0, root);
        // we compute hash of root name for the higher part of the node id
        // the upper half of the node id is u64::MAX since client IDs canonically use only 53 bits
        NodeID::new(ClientID::MAX_VALUE, hash.into())
    }

    pub fn from_nested(id: ID) -> NodeID {
        id
    }
}
