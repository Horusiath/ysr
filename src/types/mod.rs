use crate::Transaction;
use crate::block::{BlockMut, ID};
use crate::node::{Node, NodeID, NodeType};
use crate::store::Db;
use std::borrow::{Borrow, BorrowMut, Cow};
use std::marker::PhantomData;

pub mod dynamic;
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
    pub fn new(node: Node<'static>) -> Self {
        Unmounted {
            node,
            _capability: PhantomData,
        }
    }

    pub fn root<S>(name: S) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        Unmounted {
            node: Node::root_named(name),
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
    pub fn mount_mut<'tx, 'db, Txn>(
        &self,
        tx: &'tx mut Txn,
    ) -> crate::Result<Mounted<Cap, &'tx mut Transaction<'db>>>
    where
        Txn: BorrowMut<Transaction<'db>>,
    {
        let borrowed = tx.borrow_mut();
        let db = borrowed.db();
        let blocks = db.blocks();
        let block = blocks.get_or_insert_node(self.node.clone(), Cap::node_type())?;
        Ok(Mounted::new(block, borrowed))
    }

    pub fn mount<'tx, 'db, Txn>(
        &self,
        tx: &'tx Txn,
    ) -> crate::Result<Mounted<Cap, &'tx Transaction<'db>>>
    where
        Txn: Borrow<Transaction<'db>>,
    {
        let borrowed = tx.borrow();
        let db = borrowed.db();
        let blocks = db.blocks();
        let block: BlockMut = blocks.get_or_insert_node(self.node.clone(), Cap::node_type())?;
        Ok(Mounted::new(block, borrowed))
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
    block: BlockMut,
    tx: Txn,
    _capability: PhantomData<Cap>,
}

impl<Cap, Txn> Mounted<Cap, Txn> {
    pub fn new(block: BlockMut, tx: Txn) -> Self {
        Mounted {
            block,
            tx,
            _capability: PhantomData,
        }
    }

    pub fn node_id(&self) -> &NodeID {
        self.block.id()
    }

    pub fn split(self) -> (BlockMut, Txn) {
        (self.block, self.tx)
    }
}
