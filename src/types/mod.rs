use crate::node::NodeID;
use crate::Transaction;
use std::marker::PhantomData;

pub mod list;
pub mod map;
pub mod text;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unmounted<Cap> {
    node_id: NodeID,
    _capability: PhantomData<Cap>,
}

impl<Cap> Unmounted<Cap> {
    pub fn new(node_id: NodeID) -> Self {
        Unmounted {
            node_id,
            _capability: PhantomData,
        }
    }

    pub fn node_id(&self) -> NodeID {
        self.node_id
    }

    pub fn mount<'db, Txn>(&self, tx: Txn) -> crate::Result<()>
    where
        Txn: AsRef<Transaction<'db>>,
    {
        todo!()
    }
}

impl<Cap> From<NodeID> for Unmounted<Cap> {
    fn from(node_id: NodeID) -> Self {
        Unmounted::new(node_id)
    }
}

impl<Cap> From<Unmounted<Cap>> for NodeID {
    fn from(value: Unmounted<Cap>) -> Self {
        value.node_id
    }
}

#[derive(Debug)]
pub struct Mounted<Cap, Txn> {
    node: Unmounted<Cap>,
    tx: Txn,
}

impl<Cap, Txn> Mounted<Cap, Txn> {
    pub fn new(node: Unmounted<Cap>, tx: Txn) -> Self {
        Mounted { node, tx }
    }

    pub fn node_id(&self) -> NodeID {
        self.node.node_id
    }

    pub fn split(self) -> (Unmounted<Cap>, Txn) {
        (self.node, self.tx)
    }
}
