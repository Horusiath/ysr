use crate::block::BlockBuilder;
use crate::node::{Node, NodeType};
use crate::store::lmdb::store::SplitResult;
use crate::store::lmdb::BlockStore;
use crate::Clock;
use lmdb_rs_m::Database;
use std::collections::HashSet;

pub(crate) struct IntegrationContext {
    pub left: Option<BlockBuilder>,
    pub right: Option<BlockBuilder>,
    pub parent: Option<BlockBuilder>,
    pub offset: Clock,
}

impl IntegrationContext {
    pub fn create(
        target: &BlockBuilder,
        parent_name: Option<&str>,
        offset: Clock,
        db: &mut Database,
    ) -> crate::Result<Self> {
        let node = match parent_name {
            None => Node::nested(*target.parent()),
            Some(parent_name) => Node::root(parent_name),
        };
        let parent = match db.get_or_insert_node(node, NodeType::Unknown) {
            Ok(block) => Some(block),
            Err(crate::Error::NotFound) => None,
            Err(e) => return Err(e),
        };

        let left = if let Some(&origin) = target.origin_left() {
            Some(match db.split_block(origin)? {
                SplitResult::Unchanged(left) => left.into(),
                SplitResult::Split(left, _) => left,
            })
        } else {
            None
        };
        let right = if let Some(&origin) = target.origin_right() {
            Some(match db.split_block(origin)? {
                SplitResult::Unchanged(block) => block.into(),
                SplitResult::Split(_, right) => right,
            })
        } else {
            None
        };
        Ok(IntegrationContext {
            left,
            right,
            parent,
            offset,
        })
    }

    pub fn detect_conflict(&self, target: &BlockBuilder) -> bool {
        // original code: ((!target.left && (!target.right || target.right.left !== null)) || (target.left && target.left.right !== target.right))
        match (&self.left, &self.right) {
            (None, None) => true,                          // !target.left && !target.right
            (None, Some(right)) => right.left().is_some(), // !target.left && target.right.left !== null
            (Some(left), _) => left.right() != target.right(), // target.left && target.left.right !== target.right
            _ => false,
        }
    }

    pub fn resolve_conflict(&mut self, target: &mut BlockBuilder) {
        let mut o = if let Some(left) = self.left {
            left.right
        } else if let Some(sub) = &target.entry_key() {
            let mut o = self.parent.map.get(sub).cloned();
            while let Some(item) = o.as_deref() {
                if item.left.is_some() {
                    o = item.left.clone();
                    continue;
                }
                break;
            }
            o.clone()
        } else {
            self.parent.start
        };

        let mut left = target.left();
        let mut conflicting_items = HashSet::new();
        let mut items_before_origin = HashSet::new();

        // Let c in conflicting_items, b in items_before_origin
        // ***{origin}bbbb{this}{c,b}{c,b}{o}***
        // Note that conflicting_items is a subset of items_before_origin
        while let Some(item) = o {}

        /*
               // set the first conflicting item
               let mut o = if let Some(left) = left {
                   left.right
               } else if let Some(sub) = &this.parent_sub {
                   let mut o = parent_ref.map.get(sub).cloned();
                   while let Some(item) = o.as_deref() {
                       if item.left.is_some() {
                           o = item.left.clone();
                           continue;
                       }
                       break;
                   }
                   o.clone()
               } else {
                   parent_ref.start
               };

               let mut left = this.left.clone();
               let mut conflicting_items = HashSet::new();
               let mut items_before_origin = HashSet::new();

               // Let c in conflicting_items, b in items_before_origin
               // ***{origin}bbbb{this}{c,b}{c,b}{o}***
               // Note that conflicting_items is a subset of items_before_origin
               while let Some(item) = o {
                   if Some(item) == this.right {
                       break;
                   }

                   items_before_origin.insert(item);
                   conflicting_items.insert(item);
                   if this.origin == item.origin {
                       // case 1
                       if item.id.client < this.id.client {
                           left = Some(item.clone());
                           conflicting_items.clear();
                       } else if this.right_origin == item.right_origin {
                           // `self` and `item` are conflicting and point to the same integration
                           // points. The id decides which item comes first. Since `self` is to
                           // the left of `item`, we can break here.
                           break;
                       }
                   } else {
                       if let Some(origin_ptr) = item
                           .origin
                           .as_ref()
                           .and_then(|id| store.blocks.get_item(id))
                       {
                           if items_before_origin.contains(&origin_ptr) {
                               if !conflicting_items.contains(&origin_ptr) {
                                   left = Some(item.clone());
                                   conflicting_items.clear();
                               }
                           } else {
                               break;
                           }
                       } else {
                           break;
                       }
                   }
                   o = item.right.clone();
               }
               this.left = left;
        */
    }
}
