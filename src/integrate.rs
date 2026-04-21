use crate::Clock;
use crate::block::{BlockMut, InsertBlockData};
use crate::node::NodeType;
use crate::store::Db;
use crate::store::block_store::{BlockCursor, SplitResult};
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
        cursor: &mut BlockCursor<'_>,
    ) -> crate::Result<Self> {
        let left = if let Some(&origin) = target.block.origin_left() {
            let split_id = origin.add(1.into());
            Some(match cursor.split(split_id) {
                Ok(SplitResult::Split(left, _)) => left,
                // - `Unchanged`: `origin + 1` is already at a block boundary, so `origin`
                //   is the last clock of the previous block.
                // - `NotFound`: nothing contains `origin + 1`, meaning `origin` is the last
                //   clock of the last block in the list.
                // In both cases the left neighbor is the block ending at `origin`.
                Ok(SplitResult::Unchanged(_)) | Err(crate::Error::NotFound) => {
                    cursor.seek_containing(origin)?.into()
                }
                Err(e) => return Err(e),
            })
        } else {
            None
        };
        let right = if let Some(&origin) = target.block.origin_right() {
            Some(match cursor.split(origin)? {
                SplitResult::Unchanged(block) => block,
                SplitResult::Split(_, right) => right,
            })
        } else {
            None
        };

        if target.parent().is_none() {
            let parent = match &left {
                Some(left) => Some(*left.deref().parent()),
                None => right.as_ref().map(|right| *right.parent()),
            };
            if let Some(parent) = parent {
                target.block.set_parent(parent);
            }
        }
        let parent = match target.parent() {
            Some(node) => match cursor.get_or_insert_node(node.clone(), NodeType::Unknown) {
                Ok(block) => Some(block),
                Err(crate::Error::NotFound) => None,
                Err(e) => return Err(e),
            },
            None => {
                let block = cursor.seek(*target.block.parent())?;
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

    pub fn detect_conflict(&self, _target: &InsertBlockData) -> bool {
        // original code: ((!target.left && (!target.right || target.right.left !== null)) || (target.left && target.left.right !== target.right))
        match (&self.left, &self.right) {
            (None, None) => true,                          // !target.left && !target.right
            (None, Some(right)) => right.left().is_some(), // !target.left && target.right.left !== null
            (Some(left), _) => left.right() != self.right.as_ref().map(|r| r.id()), // target.left && target.left.right !== target.right
            _ => false,
        }
    }

    pub fn resolve_conflict<'tx>(
        &mut self,
        target: &mut InsertBlockData,
        cursor: &mut BlockCursor<'tx>,
    ) -> crate::Result<()> {
        let parent = self.parent.as_mut().unwrap();
        let mut o = if let Some(left) = &self.left {
            left.right().cloned()
        } else if let Some(sub) = target.entry_key() {
            let map_entries = cursor.db().map_entries();
            let mut o = map_entries.get(parent.id(), sub)?.copied();
            //let mut o = db.entry(*parent.id(), sub).optional()?.copied();
            while let Some(id) = o {
                let item = cursor.seek_containing(id)?;
                if let Some(&left) = item.left() {
                    o = Some(left);
                    continue;
                }
                break;
            }
            o
        } else {
            parent.start().copied()
        };

        let mut left = self.left.as_ref().map(|l| l.last_id());
        let mut conflicting_items = HashSet::new();
        let mut items_before_origin = HashSet::new();

        // Let c in conflicting_items, b in items_before_origin
        // ***{origin}bbbb{this}{c,b}{c,b}{o}***
        // Note that conflicting_items is a subset of items_before_origin
        while let Some(item) = o {
            if self.right.as_ref().map(|r| r.id()) == Some(&item) {
                break;
            }
            items_before_origin.insert(item);
            conflicting_items.insert(item);

            let item = cursor.seek(item)?;
            if target.block.origin_left() == item.origin_left() {
                // case 1
                let item_id = item.id();
                if item_id.client < target.id().client {
                    left = Some(*item_id);
                    conflicting_items.clear();
                } else if target.block.origin_right() == item.origin_right() {
                    // `self` and `item` are conflicting and point to the same integration
                    // points. The id decides which item comes first. Since `self` is to
                    // the left of `item`, we can break here.
                    break;
                }
            } else {
                if let Some(origin_left) = item.origin_left().and_then(|&id| cursor.seek(id).ok()) {
                    if items_before_origin.contains(origin_left.id()) {
                        if !conflicting_items.contains(origin_left.id()) {
                            left = Some(*item.id());
                            conflicting_items.clear();
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            o = item.right().cloned();
        }
        target.block.set_left(left.as_ref());

        // After conflict resolution, the block's left neighbor may have changed.
        // Update context.left to match the resolved position.
        match target.block.left() {
            Some(&left_id) => {
                let needs_update = if let Some(current_left) = self.left.as_ref() {
                    current_left.last_id() != left_id
                } else {
                    true
                };
                if needs_update {
                    self.left = cursor.seek_containing(left_id).ok().map(BlockMut::from);
                }
            }
            None => {
                self.left = None;
            }
        }

        Ok(())
    }
}
