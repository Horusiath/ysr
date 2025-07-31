use crate::block::{BlockMut, ID};
use crate::node::{NodeHeader, NodeID, NodeType};
use crate::store::lmdb::BlockStore;
use crate::Transaction;
use std::borrow::BorrowMut;
use std::marker::PhantomData;

pub mod list;
pub mod map;
pub mod text;

pub trait Capability {
    fn node_type() -> NodeType;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unmounted<Cap> {
    node_id: NodeID,
    _capability: PhantomData<Cap>,
}

impl<Cap> Unmounted<Cap> {
    fn new(node_id: NodeID) -> Self {
        Unmounted {
            node_id,
            _capability: PhantomData,
        }
    }

    pub fn root<S>(name: S) -> Self
    where
        S: AsRef<[u8]>,
    {
        Unmounted {
            node_id: NodeID::from_root(name),
            _capability: PhantomData,
        }
    }

    pub fn nested(id: ID) -> Self {
        Unmounted {
            node_id: NodeID::from_nested(id),
            _capability: PhantomData,
        }
    }

    pub fn node_id(&self) -> NodeID {
        self.node_id
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
        let block: BlockMut = match borrowed.db().block_containing(self.node_id, true) {
            Ok(block) => block.into(),
            Err(crate::Error::BlockNotFound(_)) => {
                if self.node_id.is_root() {
                    // since root nodes live forever, we can create it if it does not exist
                    let header = NodeHeader::new(Cap::node_type() as u8);
                    todo!()
                } else {
                    // nested nodes are not created automatically, if we didn't find it, we return an error
                    return Err(crate::Error::NotFound);
                }
            }
            Err(e) => return Err(e),
        };
        Ok(Mounted::new(block, tx))
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
