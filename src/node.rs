use crate::block::ID;
use crate::{ClientID, U64};
use std::fmt::Display;
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes};

pub type NodeID = ID;

#[repr(u8)]
#[derive(Debug, TryFromBytes, KnownLayout, Immutable, IntoBytes)]
pub enum NodeType {
    Unknown = 0,
    Array = 1,
    Map = 2,
    Text = 3,
    XmlFragment = 4,
    XmlElement = 5,
    XmlText = 6,
}

impl TryFrom<u8> for NodeType {
    type Error = crate::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(NodeType::Unknown),
            1 => Ok(NodeType::Array),
            2 => Ok(NodeType::Map),
            3 => Ok(NodeType::Text),
            4 => Ok(NodeType::XmlFragment),
            5 => Ok(NodeType::XmlElement),
            6 => Ok(NodeType::XmlText),
            _ => Err(crate::Error::UnknownNodeType(value)),
        }
    }
}

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

impl Display for NodeHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let node_type = NodeType::try_from(self.type_ref).unwrap_or(NodeType::Unknown);
        write!(f, "{:?}", node_type)
    }
}

#[repr(transparent)]
#[derive(FromBytes, KnownLayout, Immutable, IntoBytes)]
pub(crate) struct NodeFlags(u8);

impl NodeFlags {}

impl NodeID {
    pub fn from_root(root: &[u8]) -> NodeID {
        let hash = twox_hash::XxHash32::oneshot(0, root);
        // we compute hash of root name for the higher part of the node id
        // the upper half of the node id is u64::MAX since client IDs canonically use only 53 bits
        NodeID::new(ClientID::MAX_VALUE, hash.into())
    }

    #[inline]
    pub const fn from_nested(id: ID) -> NodeID {
        id
    }
}
