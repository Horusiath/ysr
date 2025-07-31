use crate::block::{BlockHeader, ID};
use crate::{ClientID, U64};
use std::borrow::Cow;
use std::fmt::Display;
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes};

pub type NodeID = ID;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Node<'a> {
    Root(Cow<'a, str>),
    Nested(ID),
}

impl<'a> Node<'a> {
    #[inline]
    pub fn root<S>(name: S) -> Self
    where
        S: Into<Cow<'a, str>>,
    {
        Node::Root(name.into())
    }
    #[inline]
    pub fn nested(id: ID) -> Self {
        Node::Nested(id)
    }

    #[inline]
    pub fn is_root(&self) -> bool {
        matches!(self, Node::Root(_))
    }

    #[inline]
    pub fn is_nested(&self) -> bool {
        matches!(self, Node::Nested(_))
    }

    pub fn as_str(&self) -> Option<&str> {
        if let Node::Root(name) = self {
            Some(name)
        } else {
            None
        }
    }

    pub fn id(&self) -> NodeID {
        match self {
            Node::Root(name) => NodeID::from_root(name.as_bytes()),
            Node::Nested(id) => *id,
        }
    }
}

#[repr(u8)]
#[derive(Debug, TryFromBytes, KnownLayout, Immutable, IntoBytes)]
pub enum NodeType {
    Unknown = 0,
    List = 1,
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
            1 => Ok(NodeType::List),
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
pub struct NodeHeader {
    type_ref: u8,
    flags: NodeFlags,
    start: ID,
}

impl NodeHeader {
    pub const SIZE: usize = size_of::<NodeHeader>();

    pub fn new(type_ref: u8) -> Self {
        Self {
            type_ref,
            flags: NodeFlags(0),
            start: ID::new_zeroed(),
        }
    }

    pub fn node_type(&self) -> NodeType {
        NodeType::try_from(self.type_ref).unwrap_or(NodeType::Unknown)
    }

    pub fn start(&self) -> ID {
        self.start
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
    pub fn from_root<S>(root: S) -> NodeID
    where
        S: AsRef<[u8]>,
    {
        let hash = twox_hash::XxHash32::oneshot(0, root.as_ref());
        // we compute hash of root name for the higher part of the node id
        // the upper half of the node id is u64::MAX since client IDs canonically use only 53 bits
        NodeID::new(ClientID::MAX_VALUE, hash.into())
    }

    #[inline]
    pub const fn from_nested(id: ID) -> NodeID {
        id
    }

    #[inline]
    pub fn is_root(&self) -> bool {
        self.client == ClientID::MAX_VALUE
    }

    #[inline]
    pub fn is_nested(&self) -> bool {
        !self.is_root()
    }
}
