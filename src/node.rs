use crate::block::{BlockHeader, ID};
use crate::{ClientID, U64};
use bitflags::bitflags;
use std::borrow::Cow;
use std::fmt::Display;
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes};

pub type NodeID = ID;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Node<'a> {
    Root(Named<'a>),
    Nested(ID),
}

impl<'a> Node<'a> {
    #[inline]
    pub fn root_named<S>(name: S) -> Self
    where
        S: Into<Cow<'a, str>>,
    {
        Node::Root(Named::Name(name.into()))
    }
    #[inline]
    pub fn root_hashed(node_id: NodeID) -> Self {
        Node::Root(Named::Hash(node_id))
    }

    #[inline]
    pub const fn nested(id: ID) -> Self {
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
        match self {
            Node::Root(Named::Name(name)) => Some(name),
            _ => None,
        }
    }

    pub fn id(&self) -> NodeID {
        match self {
            Node::Root(name) => name.node_id(),
            Node::Nested(id) => *id,
        }
    }

    pub fn to_owned(self) -> Node<'static> {
        match self {
            Node::Root(name) => Node::Root(name.into_owned()),
            Node::Nested(id) => Node::Nested(id),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Named<'a> {
    Name(Cow<'a, str>),
    Hash(NodeID),
}

impl Named<'static> {
    pub fn new_hashed(name: &str) -> Self {
        let hash = NodeID::from_root(name.as_bytes());
        Self::Hash(hash)
    }
}

impl<'a> Named<'a> {
    pub fn into_owned(self) -> Named<'static> {
        match self {
            Named::Name(cow) => Named::Name(Cow::Owned(cow.into_owned())),
            Named::Hash(hash) => Named::Hash(hash),
        }
    }

    pub fn as_hashed(&self) -> Named<'static> {
        Named::Hash(self.node_id())
    }

    pub fn node_id(&self) -> NodeID {
        match self {
            Named::Hash(id) => *id,
            Named::Name(name) => NodeID::from_root(name.as_bytes()),
        }
    }
}

#[repr(u8)]
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, TryFromBytes, KnownLayout, Immutable, IntoBytes, Default,
)]
pub enum NodeType {
    #[default]
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

impl Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::Unknown => write!(f, "Unknown"),
            NodeType::List => write!(f, "List"),
            NodeType::Map => write!(f, "Map"),
            NodeType::Text => write!(f, "Text"),
            NodeType::XmlFragment => write!(f, "XmlFragment"),
            NodeType::XmlElement => write!(f, "XmlElement"),
            NodeType::XmlText => write!(f, "XmlText"),
        }
    }
}

#[repr(transparent)]
#[derive(FromBytes, KnownLayout, Immutable, IntoBytes, Default)]
pub(crate) struct NodeFlags(u8);

bitflags! {
    impl NodeFlags : u8 {
        const HAS_START = 0b0000_0001;
    }
}

impl NodeID {
    pub fn from_root<S>(root: S) -> NodeID
    where
        S: AsRef<[u8]>,
    {
        let hash = twox_hash::XxHash32::oneshot(0, root.as_ref());
        // we compute hash of root name for the higher part of the node id
        // the upper half of the node id is u64::MAX since client IDs canonically use only 53 bits
        NodeID::new(ClientID::ROOT, hash.into())
    }

    #[inline]
    pub const fn from_nested(id: ID) -> NodeID {
        id
    }

    #[inline]
    pub fn is_root(&self) -> bool {
        self.client == ClientID::ROOT
    }

    #[inline]
    pub fn is_nested(&self) -> bool {
        !self.is_root()
    }
}
