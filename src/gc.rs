use crate::block::BlockFlags;
use crate::block_reader::BlockRange;
use crate::content::ContentType;
use crate::id_set::IDSet;
use crate::store::Db;
use crate::transaction::TxMutScope;
use crate::{Block, BlockMut, ID, Optional};

pub struct GarbageCollector<'tx> {
    tx: TxMutScope<'tx>,
}

impl<'tx> GarbageCollector<'tx> {
    pub fn new(tx: TxMutScope<'tx>) -> Self {
        GarbageCollector { tx }
    }

    pub fn collect(&mut self, ds: &IDSet) -> crate::Result<()> {
        for (&client, id_range) in ds.iter() {
            'client_ranges: for range in id_range.iter() {
                let mut clock = range.start;

                // set cursor at first block >= current id/range
                self.tx.cursor.start_from(ID::new(client, clock))?;
                let mut current = self.tx.cursor.current().optional()?;
                // iterate until the end of current range, since it may span over multiple blocks
                while clock <= range.end {
                    if let Some(block) = current {
                        let id = block.id();
                        if id.client != client {
                            // we moved over the current client -> jump to next client
                            break 'client_ranges;
                        }
                        if !range.contains(&id.clock) {
                            // we moved over the current range -> jump to next range (same client)
                            continue 'client_ranges;
                        }

                        let block_len = block.clock_len();
                        self.gc_block(&block, false)?;
                        clock += block_len;
                        if clock <= range.end {
                            // current block didn't reach the end of current range
                            // move to the next block
                            current = self.tx.cursor.next()?;
                        }
                    } else {
                        // no more blocks
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    fn gc_block(&mut self, block: &Block<'tx>, parent_gc: bool) -> crate::Result<bool> {
        if block.is_deleted() {
            let len = block.clock_len();

            if block.content_type() == ContentType::Node {
                self.gc_node(block)?;
            }

            if !block.flags().contains(BlockFlags::INLINE_CONTENT) {
                let contents = self.tx.db.contents();
                contents.delete_range(block.content_type(), &BlockRange::new(*block.id(), len))?;
            }

            if parent_gc {
                // delete permanently
                self.tx.cursor.remove(*block.id())?;
            } else {
                // soft delete
                let mut block = BlockMut::from(*block);
                block.clear_inline_content();
                block.flags().remove(BlockFlags::COUNTABLE);
                block.set_content_type(ContentType::Deleted);
                self.tx.cursor.update(block.as_block())?;
                self.tx.state.merge_blocks.insert(*block.id());
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn gc_node(&mut self, node: &Block) -> crate::Result<()> {
        let mut current = node.start().copied();
        while let Some(id) = current.take() {
            // remove all list-like entries
            let block = self.tx.cursor.seek(id)?;
            self.gc_block(&block, true)?;
            current = block.right().copied();
        }

        // remove all map-like entries
        let map_entries = self.tx.db.map_entries();
        let mut iter = map_entries.entries(node.id());
        while iter.next()?.is_some() {
            let block_id = iter.block_id()?;
            let mut current = self.tx.cursor.seek(*block_id)?;
            self.gc_block(&current, true)?;

            // remove all previous versions of the entry
            while let Some(left_id) = current.left() {
                current = self.tx.cursor.seek(*left_id)?;
                self.gc_block(&current, true)?;
            }
        }
        map_entries.remove_all(node.id())?;

        Ok(())
    }
}
