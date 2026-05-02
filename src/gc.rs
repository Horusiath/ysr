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
                let range = BlockRange::new(*block.id(), len);
                contents.delete_range(block.content_type(), &range)?;
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
            current = block.right().copied();
            self.gc_block(&block, true)?;
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

#[cfg(test)]
mod test {
    use crate::content::ContentType;
    use crate::store::Db;
    use crate::test_util::multi_doc;
    use crate::types::text::TextPrelim;
    use crate::{BlockMut, ID, In, List, ListPrelim, Map, MapPrelim, Optional, Unmounted, lib0};

    const CLIENT: u32 = 1;

    fn id(clock: u32) -> ID {
        ID::new(CLIENT.into(), clock.into())
    }

    fn block(tx: &crate::Transaction<'_>, id: ID) -> Option<BlockMut> {
        tx.db
            .get()
            .blocks()
            .get(id)
            .optional()
            .unwrap()
            .map(BlockMut::from)
    }

    fn content_exists(tx: &crate::Transaction<'_>, id: ID) -> bool {
        tx.db.get().contents().get(id).optional().unwrap().is_some()
    }

    fn count_map_entries(tx: &crate::Transaction<'_>, node_id: ID) -> usize {
        let mut iter = tx.db.get().map_entries().entries(&node_id);
        let mut n = 0;
        while iter.next().unwrap().is_some() {
            n += 1;
        }
        n
    }

    #[test]
    fn gc_tombstones_list() {
        let list: Unmounted<List> = Unmounted::root("list");
        let (doc, _dir) = multi_doc(CLIENT);

        let mut tx = doc.transact_mut("test").unwrap();
        {
            let mut l = list.mount_mut(&mut tx).unwrap();
            l.push_back(true).unwrap(); // id(0)
            l.push_back(false).unwrap(); // id(1)
            l.push_back(42).unwrap(); // id(2)
        }
        tx.commit(None).unwrap();

        // even though [true, false, 42] are individually inlineable, due to block merge
        // they should get their own contents
        let mut tx = doc.transact_mut("test").unwrap();
        let b = block(&tx, id(0)).unwrap();
        assert_eq!(b.clock_len().get(), 3, "content block should be squashed");
        assert!(content_exists(&tx, id(0)), "'true' content exists");
        assert!(content_exists(&tx, id(1)), "'false' content exists");
        assert!(content_exists(&tx, id(2)), "'42' content exists");

        {
            let mut l = list.mount_mut(&mut tx).unwrap();
            l.remove_range(0..3).unwrap();
        }

        let ds = tx.delete_set().cloned().unwrap_or_default();
        tx.gc(&ds).unwrap();
        tx.commit(None).unwrap();

        let tx = doc.transact("test").unwrap();
        // sequential updates were merged, so we only have one block
        let block = block(&tx, id(0)).unwrap();
        assert_eq!(block.content_type(), ContentType::Deleted);
        assert_eq!(block.clock_len().get(), 3);

        for clock in 0..3 {
            // content should be deleted regardless of soft/hard delete
            assert!(!content_exists(&tx, id(clock)));
        }
        tx.commit(None).unwrap();
    }

    #[test]
    fn gc_tombstones_map() {
        let map: Unmounted<Map> = Unmounted::root("map");
        let (doc, _dir) = multi_doc(CLIENT);

        let mut tx = doc.transact_mut("test").unwrap();
        let map_node_id;
        {
            let mut m = map.mount_mut(&mut tx).unwrap();
            map_node_id = *m.node_id();
            m.insert("key1", true).unwrap(); // id(0), inline atom
            m.insert("key2", false).unwrap(); // id(1), inline atom
            m.insert("key3", lib0!({"first_name": "John", "last_name": "Smith"}))
                .unwrap(); // id(2), atom on content store
        }

        assert!(content_exists(&tx, id(2)));
        assert_eq!(count_map_entries(&tx, map_node_id), 3);

        {
            let mut m = map.mount_mut(&mut tx).unwrap();
            m.remove("key1").unwrap();
            m.remove("key2").unwrap();
            m.remove("key3").unwrap();
        }

        let ds = tx.delete_set().cloned().unwrap_or_default();
        tx.gc(&ds).unwrap();
        tx.commit(None).unwrap();

        let tx = doc.transact_mut("test").unwrap();
        for clock in 0..3 {
            // Blocks still exist but tombstoned
            let block = block(&tx, id(clock)).unwrap();
            assert_eq!(block.content_type(), ContentType::Deleted);

            // regardless of soft/hard delete content should be removed
            assert!(!content_exists(&tx, id(clock)));
        }
        tx.commit(None).unwrap();
    }

    #[test]
    fn gc_nested_map_hard_deletes_entries() {
        let list: Unmounted<List> = Unmounted::root("list");
        let (doc, _dir) = multi_doc(CLIENT);

        let mut tx = doc.transact_mut("test").unwrap();
        let nested_map;
        {
            let mut l = list.mount_mut(&mut tx).unwrap();
            nested_map = l
                .insert(
                    0,
                    MapPrelim::from_iter([
                        ("name".into(), In::from("Alice")), // id(1)
                        ("age".into(), In::from(30)),       // id(2)
                    ]),
                )
                .unwrap();
        }
        let nested_map_id = nested_map.node_id(); // id(0)
        assert_eq!(count_map_entries(&tx, nested_map_id), 2);

        {
            let mut l = list.mount_mut(&mut tx).unwrap();
            l.remove(0).unwrap();
        }

        let ds = tx.delete_set().cloned().unwrap_or_default();
        tx.gc(&ds).unwrap();
        tx.commit(None).unwrap();

        let tx = doc.transact_mut("test").unwrap();
        // Map node block is tombstoned
        let nested_map_block = block(&tx, nested_map_id).unwrap();
        assert_eq!(nested_map_block.content_type(), ContentType::Deleted);
        // Child entry blocks hard-deleted
        assert!(block(&tx, id(1)).is_none(), "'name' permanently deleted");
        assert!(block(&tx, id(2)).is_none(), "'age' permanently deleted");
        // Map entries store cleared
        assert_eq!(count_map_entries(&tx, nested_map_id), 0);
        tx.commit(None).unwrap();
    }

    #[test]
    fn gc_nested_list_hard_deletes_items() {
        let root: Unmounted<Map> = Unmounted::root("root");
        let (doc, _dir) = multi_doc(CLIENT);

        let mut tx = doc.transact_mut("test").unwrap();
        let nested_list;
        {
            let mut m = root.mount_mut(&mut tx).unwrap();
            nested_list = m
                .insert(
                    "items",                                                    // id(0) for prelim itself
                    ListPrelim::from(vec!["x".into(), "y".into(), "z".into()]), // id(1), id(2), id(3) for items
                )
                .unwrap();
        }
        let nested_list_id = nested_list.node_id();

        {
            let mut m = root.mount_mut(&mut tx).unwrap();
            m.remove("items").unwrap();
        }

        let ds = tx.delete_set().cloned().unwrap_or_default();
        tx.gc(&ds).unwrap();
        tx.commit(None).unwrap();

        let tx = doc.transact_mut("test").unwrap();
        // List node tombstoned
        let list_node_block = block(&tx, nested_list_id).unwrap();
        assert_eq!(list_node_block.content_type(), ContentType::Deleted);
        // all children should be hard deleted
        assert!(block(&tx, id(1)).is_none(), "'x' permanently deleted");
        assert!(block(&tx, id(2)).is_none(), "'y' permanently deleted");
        assert!(block(&tx, id(3)).is_none(), "'z' permanently deleted");
        tx.commit(None).unwrap();
    }

    #[test]
    fn gc_removes_left_neighbor_versions_of_map_entries() {
        let list: Unmounted<List> = Unmounted::root("list");
        let (doc, _dir) = multi_doc(CLIENT);

        let mut tx = doc.transact_mut("test").unwrap();
        let child_map;
        {
            let mut l = list.mount_mut(&mut tx).unwrap();
            child_map = l.insert(0, MapPrelim::default()).unwrap(); // id(0)
        }
        let child_map_id = child_map.node_id();

        {
            let mut m = child_map.mount_mut(&mut tx).unwrap();
            m.insert("key", 100).unwrap(); // id(1), inline atom
            m.insert("key", 200).unwrap(); // id(2), inline atom
            m.insert("key", 300).unwrap(); // id(3), inline atom
        }

        for clock in 1..=3 {
            assert!(block(&tx, id(clock)).is_some(), "block {clock} exists");
        }

        {
            let mut l = list.mount_mut(&mut tx).unwrap();
            l.remove(0).unwrap();
        }

        let ds = tx.delete_set().cloned().unwrap_or_default();
        tx.gc(&ds).unwrap();
        tx.commit(None).unwrap();

        let tx = doc.transact_mut("test").unwrap();
        // Map node block tombstoned
        let nested_map_block = block(&tx, id(0)).unwrap();
        assert_eq!(nested_map_block.content_type(), ContentType::Deleted);
        // All entry version blocks hard-deleted (children via gc_node)
        for clock in 1..=3 {
            let block = block(&tx, id(clock));
            assert!(block.is_none(), "block({clock}) should be hard-deleted",);
        }
        // Map entries store cleaned up
        assert_eq!(count_map_entries(&tx, child_map_id), 0);
        tx.commit(None).unwrap();
    }

    #[test]
    fn gc_only_affects_blocks_within_delete_set_ranges() {
        let list: Unmounted<List> = Unmounted::root("list");
        let (doc, _dir) = multi_doc(CLIENT);

        let mut tx = doc.transact_mut("test").unwrap();
        {
            let mut l = list.mount_mut(&mut tx).unwrap();
            l.push_back("a").unwrap(); // id(0)
            l.push_back("b").unwrap(); // id(1)
            l.push_back("c").unwrap(); // id(2)
            l.push_back("d").unwrap(); // id(3)
        }
        tx.commit(None).unwrap(); // blocks [0..3] merged together

        // items pushed sequentially will be merged into one block
        let mut tx = doc.transact_mut("test").unwrap();
        let b = block(&tx, id(0)).unwrap();
        assert_eq!(
            b.clock_len().get(),
            4,
            "items pushed sequentially should be merged into one block"
        );
        for clock in 0..4 {
            assert!(content_exists(&tx, id(clock)), "content_exists: {}", clock);
        }

        {
            let mut l = list.mount_mut(&mut tx).unwrap();
            l.remove(1).unwrap(); // remove "b", split block into ["a"], ["c", "d"]
        }

        let ds = tx.delete_set().cloned().unwrap_or_default();
        tx.gc(&ds).unwrap();
        tx.commit(None).unwrap();

        let tx = doc.transact("test").unwrap();
        // "b" tombstoned
        let b = block(&tx, id(1)).unwrap();
        assert_eq!(b.content_type(), ContentType::Deleted);

        // ["a"], ["c", "d"] untouched — blocks exist and are NOT tombstoned
        for (clock, len) in [(0, 1), (2, 2)] {
            let block = block(&tx, id(clock)).unwrap();
            assert_ne!(block.content_type(), ContentType::Deleted);
            assert_eq!(block.clock_len().get(), len);
            assert!(content_exists(&tx, id(clock)), "content_exists: {}", clock);
        }
        tx.commit(None).unwrap();
    }

    #[test]
    fn gc_text_node_hard_deletes_text_content_blocks() {
        // clock layout: TextNode -> id(0), text "some text here" (14 utf-16 chars) -> id(1)
        let root: Unmounted<Map> = Unmounted::root("root");
        let (doc, _dir) = multi_doc(CLIENT);

        let mut tx = doc.transact_mut("test").unwrap();
        let text_node;
        {
            let mut m = root.mount_mut(&mut tx).unwrap();
            text_node = m.insert("content", TextPrelim::default()).unwrap(); // id(0)
        }

        {
            let mut t = text_node.mount_mut(&mut tx).unwrap();
            t.insert(0, "some text here").unwrap(); // id(1), 14 utf-16 codepoints
        }

        assert!(content_exists(&tx, id(1)));

        {
            let mut m = root.mount_mut(&mut tx).unwrap();
            m.remove("content").unwrap();
        }

        let ds = tx.delete_set().cloned().unwrap_or_default();
        tx.gc(&ds).unwrap();
        tx.commit(None).unwrap();

        let tx = doc.transact_mut("test").unwrap();
        // Text content block hard-deleted (child of node being GC'd)
        let block = block(&tx, id(1));
        assert!(block.is_none(), "content block should be hard-deleted");
        assert!(!content_exists(&tx, id(1)));
        tx.commit(None).unwrap();
    }
}
