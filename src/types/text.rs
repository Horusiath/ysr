use crate::content::BlockContent;
use crate::node::NodeType;
use crate::store::lmdb::BlockStore;
use crate::types::Capability;
use crate::{Mounted, Transaction};
use std::fmt::{Display, Formatter};
use std::ops::{Deref, RangeBounds};

pub type TextRef<Txn> = Mounted<Text, Txn>;

#[derive(Clone, Debug, Default, Eq, Ord, PartialOrd, PartialEq)]
pub struct Text;

impl Capability for Text {
    fn node_type() -> NodeType {
        NodeType::Text
    }
}

impl<'tx, 'db> TextRef<&'tx Transaction<'db>> {
    pub fn len(&self) -> usize {
        self.block.clock_len().get() as usize
    }
}

impl<'tx, 'db> Display for TextRef<&'tx Transaction<'db>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Ok(BlockContent::Node(node)) = self.block.content() else {
            return Err(std::fmt::Error);
        };
        let mut next = node.header().start().cloned();
        let db = self.tx.db();
        while let Some(id) = next {
            let Ok(block) = db.block_containing(id, false) else {
                return Err(std::fmt::Error);
            };
            if block.is_countable() && !block.is_deleted() {
                let Ok(BlockContent::Text(chunk)) = block.content() else {
                    return Err(std::fmt::Error);
                };
                write!(f, "{}", chunk)?;
            }
            next = block.right().cloned();
        }

        Ok(())
    }
}

impl<'tx, 'db> TextRef<&'tx mut Transaction<'db>> {
    pub fn insert<S>(&mut self, index: usize, chunk: S) -> crate::Result<()>
    where
        S: AsRef<str>,
    {
        todo!()
    }

    pub fn remove_range<R>(&mut self, range: R) -> crate::Result<()>
    where
        R: RangeBounds<usize>,
    {
        todo!()
    }
}

impl<'tx, 'db> Deref for TextRef<&'tx mut Transaction<'db>> {
    type Target = TextRef<&'tx Transaction<'db>>;

    fn deref(&self) -> &Self::Target {
        // Assuming that the mutable reference can be dereferenced to an immutable reference
        // This is a common pattern in Rust to allow shared access to the same data
        unsafe { &*(self as *const _ as *const TextRef<&'tx Transaction<'db>>) }
    }
}
