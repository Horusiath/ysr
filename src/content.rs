use crate::block::{
    CONTENT_TYPE_ATOM, CONTENT_TYPE_BINARY, CONTENT_TYPE_DELETED, CONTENT_TYPE_DOC,
    CONTENT_TYPE_EMBED, CONTENT_TYPE_FORMAT, CONTENT_TYPE_NODE, CONTENT_TYPE_STRING,
};
use crate::node::NodeID;

#[repr(u8)]
pub(crate) enum BlockContent<'a> {
    Atom(&'a [u8]) = CONTENT_TYPE_ATOM,
    Binary(&'a [u8]) = CONTENT_TYPE_BINARY,
    Embed(&'a [u8]) = CONTENT_TYPE_EMBED,
    Deleted(u64) = CONTENT_TYPE_DELETED,
    Text(&'a str) = CONTENT_TYPE_STRING,
    Node(NodeID) = CONTENT_TYPE_NODE,
    Format(&'a str, &'a [u8]) = CONTENT_TYPE_FORMAT,
    Doc(&'a [u8]) = CONTENT_TYPE_DOC,
    // to be supported..
    // Move(&'a Move) = CONTENT_TYPE_MOVE,
}

impl<'a> BlockContent<'a> {
    pub fn size_hint(&self) -> usize {
        match self {
            BlockContent::Atom(v) => v.len(),
            BlockContent::Binary(v) => v.len(),
            BlockContent::Embed(v) => v.len(),
            BlockContent::Deleted(_) => size_of::<u64>(),
            BlockContent::Text(v) => v.len(),
            BlockContent::Node(_) => size_of::<NodeID>(),
            BlockContent::Format(a, v) => a.len() + v.len() + 1,
            BlockContent::Doc(v) => v.len(),
        }
    }

    pub fn content_type(&self) -> u8 {
        match self {
            BlockContent::Atom(_) => CONTENT_TYPE_ATOM,
            BlockContent::Binary(_) => CONTENT_TYPE_BINARY,
            BlockContent::Deleted(_) => CONTENT_TYPE_DELETED,
            BlockContent::Doc(_) => CONTENT_TYPE_DOC,
            BlockContent::Embed(_) => CONTENT_TYPE_EMBED,
            BlockContent::Format(_, _) => CONTENT_TYPE_FORMAT,
            BlockContent::Node(_) => CONTENT_TYPE_NODE,
            BlockContent::Text(_) => CONTENT_TYPE_STRING,
        }
    }
}
