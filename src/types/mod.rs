use crate::block::{BlockBuilder, ID};
use crate::content::{BlockContent, ContentNode};
use crate::node::{Node, NodeHeader, NodeID, NodeType};
use crate::store::lmdb::BlockStore;
use crate::Transaction;
use std::borrow::{BorrowMut, Cow};
use std::marker::PhantomData;

pub mod list;
pub mod map;
pub mod text;

pub trait Capability {
    fn node_type() -> NodeType;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unmounted<Cap> {
    node: Node<'static>,
    _capability: PhantomData<Cap>,
}

impl<Cap> Unmounted<Cap> {
    pub fn root<S>(name: S) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        Unmounted {
            node: Node::root(name),
            _capability: PhantomData,
        }
    }

    pub fn nested(id: ID) -> Self {
        Unmounted {
            node: Node::nested(id),
            _capability: PhantomData,
        }
    }

    pub fn node_id(&self) -> NodeID {
        self.node.id()
    }
}

impl<Cap> Unmounted<Cap>
where
    Cap: Capability,
{
    pub fn mount<'db, Txn>(self, mut tx: Txn) -> crate::Result<Mounted<Cap, Txn>>
    where
        Txn: BorrowMut<Transaction<'db>>,
    {
        let borrowed = tx.borrow_mut();
        let block = borrowed
            .db()
            .get_or_insert_node(self.node, Cap::node_type())?;
        Ok(Mounted::new(block, tx))
    }
}

impl<Cap> From<ID> for Unmounted<Cap> {
    fn from(node_id: ID) -> Self {
        Unmounted::nested(node_id)
    }
}

impl<Cap> From<Unmounted<Cap>> for NodeID {
    fn from(value: Unmounted<Cap>) -> Self {
        value.node_id()
    }
}

#[derive(Debug)]
pub struct Mounted<Cap, Txn> {
    block: BlockBuilder,
    tx: Txn,
    _capability: PhantomData<Cap>,
}

impl<Cap, Txn> Mounted<Cap, Txn> {
    pub fn new(block: BlockBuilder, tx: Txn) -> Self {
        Mounted {
            block,
            tx,
            _capability: PhantomData,
        }
    }

    pub fn node_id(&self) -> &NodeID {
        self.block.id()
    }

    pub fn split(self) -> (BlockBuilder, Txn) {
        (self.block, self.tx)
    }
}
