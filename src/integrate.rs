use crate::block::{Block, BlockFlags, BlockMut, InsertBlockData};
use crate::content::BlockContentMut;
use crate::node::{Node, NodeType};
use crate::store::lmdb::store::SplitResult;
use crate::store::lmdb::BlockStore;
use crate::{Clock, Optional};
use lmdb_rs_m::Database;
use std::collections::HashSet;
use std::ops::Deref;

pub(crate) struct IntegrationContext {
    pub left: Option<BlockMut>,
    pub right: Option<BlockMut>,
    pub parent: Option<BlockMut>,
    pub offset: Clock,
}

impl IntegrationContext {
    pub fn create(
        target: &mut InsertBlockData,
        offset: Clock,
        db: &mut Database,
    ) -> crate::Result<Self> {
        let left = if let Some(&origin) = target.block.origin_left() {
            Some(match db.split_block(origin)? {
                SplitResult::Unchanged(left) => left.into(),
                SplitResult::Split(left, _) => left,
            })
        } else {
            None
        };
        let right = if let Some(&origin) = target.block.origin_right() {
            Some(match db.split_block(origin)? {
                SplitResult::Unchanged(block) => block.into(),
                SplitResult::Split(_, right) => right,
            })
        } else {
            None
        };

        if target.parent().is_none() {
            let parent = match &left {
                Some(left) => Some(*left.deref().parent()),
                None => match &right {
                    None => None,
                    Some(right) => Some(*right.deref().parent()),
                },
            };
            if let Some(parent) = parent {
                target.block.set_parent(parent);
            }
        }
        let parent = match target.parent() {
            Some(node) => match db.get_or_insert_node(node.clone(), NodeType::Unknown) {
                Ok(block) => Some(block),
                Err(crate::Error::NotFound) => None,
                Err(e) => return Err(e),
            },
            None => {
                let block = db.fetch_block(*target.block.parent(), true)?;
                Some(block.into())
            }
        };
        Ok(IntegrationContext {
            left,
            right,
            parent,
            offset,
        })
    }

    pub fn detect_conflict(&self, target: &InsertBlockData) -> bool {
        // original code: ((!target.left && (!target.right || target.right.left !== null)) || (target.left && target.left.right !== target.right))
        match (&self.left, &self.right) {
            (None, None) => true,                          // !target.left && !target.right
            (None, Some(right)) => right.left().is_some(), // !target.left && target.right.left !== null
            (Some(left), _) => left.right() != target.block.right(), // target.left && target.left.right !== target.right
            _ => false,
        }
    }

    pub fn resolve_conflict(
        &mut self,
        target: &mut InsertBlockData,
        db: &Database,
    ) -> crate::Result<()> {
        let parent = self.parent.as_mut().unwrap();
        let mut o = if let Some(left) = &self.left {
            left.right().cloned()
        } else if let Some(sub) = target.entry_key() {
            let mut o = db.entry(*parent.id(), sub).optional()?;
            while let Some(id) = o {
                let item = db.fetch_block(id, true)?;
                if let Some(left) = item.left() {
                    o = Some(*left);
                    continue;
                }
                break;
            }
            o.clone()
        } else {
            parent.start().cloned()
        };

        let mut left = target.block.left().cloned();
        let mut conflicting_items = HashSet::new();
        let mut items_before_origin = HashSet::new();

        // Let c in conflicting_items, b in items_before_origin
        // ***{origin}bbbb{this}{c,b}{c,b}{o}***
        // Note that conflicting_items is a subset of items_before_origin
        while let Some(item) = o {
            if Some(&item) == target.block.right() {
                break;
            }
            items_before_origin.insert(item.clone());
            conflicting_items.insert(item.clone());

            let item = db.fetch_block(item, true)?;
            if target.block.origin_left() == item.origin_left() {
                // case 1
                let item_id = item.id();
                if item_id.client < target.id().client {
                    left = Some(item_id.clone());
                    conflicting_items.clear();
                } else if target.block.origin_right() == item.origin_right() {
                    // `self` and `item` are conflicting and point to the same integration
                    // points. The id decides which item comes first. Since `self` is to
                    // the left of `item`, we can break here.
                    break;
                }
            } else {
                if let Some(origin_left) = item
                    .origin_left()
                    .and_then(|&id| db.fetch_block(id, true).ok())
                {
                    if items_before_origin.contains(&origin_left.id()) {
                        if !conflicting_items.contains(&origin_left.id()) {
                            left = Some(origin_left.id().clone());
                            conflicting_items.clear();
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
                o = item.right().cloned();
            }

            target.block.set_left(left.as_ref());
        }

        Ok(())
    }
}
