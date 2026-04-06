use crate::block::{Block, BlockMut, ID, InsertBlockData};
use crate::block_reader::{BlockRange, Carrier, Update};
use crate::content::{ContentType, FormatAttribute};
use crate::id_set::IDSet;
use crate::integrate::IntegrationContext;
use crate::lmdb::{Database, Dbi, RwTxn};
use crate::node::{Node, NodeID};
use crate::prelim::Prelim;
use crate::read::Decoder;
use crate::state_vector::Snapshot;
use crate::store::block_store::{BlockCursor, BlockStore};
use crate::store::content_store::ContentStore;
use crate::store::intern_strings::InternStringsStore;
use crate::store::map_entries::MapEntries;
use crate::store::{Db, MapEntriesStore};
use crate::write::{Encode, Encoder, EncoderV1, WriteExt};
use crate::{ClientID, Clock, Optional, StateVector, U32};
use bitflags::bitflags;
use bytes::{BufMut, Bytes, BytesMut};
use smallvec::{SmallVec, smallvec};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::ops::Deref;
use zerocopy::IntoBytes;

pub(crate) struct TransactionState {
    pub client_id: ClientID,
    pub begin_state: StateVector,
    pub current_state: StateVector,
    pub origin: Option<Origin>,
    pub delete_set: IDSet,
    pub changed: HashMap<NodeID, HashSet<U32>>,
    pub merge_blocks: BTreeSet<ID>,
}

impl TransactionState {
    fn new(client_id: ClientID, begin_state: StateVector, origin: Option<Origin>) -> Self {
        let current_state = begin_state.clone();
        TransactionState {
            client_id,
            begin_state,
            current_state,
            origin,
            delete_set: IDSet::default(),
            changed: HashMap::default(),
            merge_blocks: BTreeSet::default(),
        }
    }

    pub fn next_id(&mut self, clock_len: Clock) -> ID {
        let clock = self.current_state.inc_by(self.client_id, clock_len);
        ID::new(self.client_id, clock)
    }

    pub(crate) fn add_changed_type(
        &mut self,
        parent_id: NodeID,
        parent_deleted: bool,
        key_hash: Option<&U32>,
    ) {
        if parent_id.is_root()
            || (parent_id.clock < self.begin_state.get(&parent_id.client) && !parent_deleted)
        {
            let e = self.changed.entry(parent_id).or_default();
            if let Some(key_hash) = key_hash {
                e.insert(*key_hash);
            }
        }
    }

    fn delete_list_members<'tx>(
        &mut self,
        start: ID,
        block_cursor: &mut BlockCursor<'tx>,
        map_entries: &MapEntriesStore<'tx>,
    ) -> crate::Result<()> {
        let mut current = Some(start);
        while let Some(id) = current {
            let mut block: BlockMut = block_cursor.seek(id)?.into();
            if !self.delete(&mut block, true, block_cursor, map_entries)?
                && block.id().clock < self.begin_state.get(&block.id().client)
            {
                // This will be gc'd later, and we want to merge it if possible
                // We try to merge all deleted items after each transaction,
                // but we have no knowledge about that this needs to be merged
                // since it is not in transaction.ds. Hence, we add it to transaction._mergeStructs
                self.merge_blocks.insert(*block.id());
            }
            current = block.right().copied();
        }
        Ok(())
    }

    fn delete_map_members<'tx>(
        &mut self,
        id: &ID,
        block_cursor: &mut BlockCursor<'tx>,
        map_entries: &MapEntriesStore<'tx>,
    ) -> crate::Result<()> {
        let mut to_delete = Vec::new();
        let mut entries = map_entries.entries(id);
        while let Some(_) = entries.next()? {
            let child_id = *entries.block_id()?;
            to_delete.push(child_id);
        }

        for entry_id in to_delete {
            let mut block: BlockMut = block_cursor.seek(entry_id)?.into();
            let existed_before = entry_id.clock < self.begin_state.get(&block.id().client);
            let deleted = self.delete(&mut block, true, block_cursor, map_entries)?;
            if deleted && existed_before {
                // same as above
                self.merge_blocks.insert(entry_id);
            }
        }
        Ok(())
    }

    pub(crate) fn delete<'tx>(
        &mut self,
        block: &mut BlockMut,
        parent_deleted: bool,
        block_cursor: &mut BlockCursor<'tx>,
        map_entries: &MapEntriesStore<'tx>,
    ) -> crate::Result<bool> {
        if block.is_deleted() {
            return Ok(false);
        }
        block.set_deleted();
        block_cursor.update(block.as_block())?;

        self.delete_set.insert(*block.id(), block.clock_len());
        self.add_changed_type(*block.parent(), parent_deleted, block.key_hash());

        match block.content_type() {
            ContentType::Node => {
                // iterate over list values of the node and delete them
                if let Some(start) = block.start() {
                    self.delete_list_members(*start, block_cursor, map_entries)?;
                }
                //iterate over map entries of the node and delete them
                self.delete_map_members(block.id(), block_cursor, map_entries)?;
            }
            _ => { /* not used */ }
        }
        Ok(true)
    }

    fn precommit(
        &mut self,
        db: Database<'_>,
        summary: Option<&mut TransactionSummary>,
    ) -> crate::Result<()> {
        // squash delete set
        self.delete_set.squash();
        let blocks = db.blocks();

        // transaction.afterState = getStateVector(transaction.doc.store)

        if let Some(summary) = summary {
            if summary.flags.contains(CommitFlags::OBSERVE_NODES) {
                // gather info about which nodes have changed
                todo!();
                if summary.flags.contains(CommitFlags::OBSERVE_NODES_DEEP) {
                    // bubble up changes to parent nodes and gather them as well
                    todo!();
                }
            }
        }

        //if (doc.gc) {
        //  tryGcDeleteSet(ds, store, doc.gcFilter)
        //}
        //tryMergeDeleteSet(ds, store)

        // on all affected store.clients props, try to merge
        let mut cursor = blocks.cursor()?;
        let mut key_changes = BTreeMap::new();
        for (client, &clock) in self.current_state.iter() {
            let before_clock = self.begin_state.get(client);
            if before_clock != clock {
                cursor.seek_containing(ID::new(*client, before_clock))?;
                Self::merge_with_lefts(&mut cursor, &mut key_changes)?;
            }
        }

        // try to merge mergeStructs

        // persist updated state vector
        let mut sv_store = db.state_vector();
        for (client, &clock) in self.current_state.iter() {
            sv_store.update(*client, clock)?;
        }

        // create incremental update

        //TODO: subdoc events

        Ok(())
    }

    /// Moving cursor right to left, try to merge structs with their left neighbors.
    /// Returns ID of the current position after merging.
    /// Expects that cursor is set within a block keyspace position.
    fn merge_with_lefts(
        cursor: &mut BlockCursor<'_>,
        key_changes: &mut BTreeMap<(NodeID, U32, ID), ID>,
    ) -> crate::Result<ID> {
        let mut right: BlockMut = cursor.current()?.into();
        let end = *right.id();
        let mut left = cursor.prev()?.map(BlockMut::from);
        while let Some(mut curr) = left {
            if curr.merge(right.as_block()) {
                if let Some(&parent_sub) = right.key_hash() {
                    let e = key_changes
                        .entry((*right.parent(), parent_sub, *right.id()))
                        .or_insert(*curr.id());
                    *e = *curr.id(); // update key
                }
                //TODO: delete right
            } else {
                break; // we couldn't merge left and right blocks
            }

            // move to left block
            left = cursor.prev()?.map(BlockMut::from);
            right = curr;
        }

        Ok(*right.id())
    }
}

pub struct DbHandle<'db> {
    txn: RwTxn<'db>,
    handle: Dbi,
}

impl<'db> DbHandle<'db> {
    pub fn get(&self) -> Database<'_> {
        self.txn.bind(&self.handle)
    }

    pub(crate) fn commit(self) -> crate::Result<()> {
        self.txn.commit()?;
        Ok(())
    }
}

pub struct LazyState {
    inner: Option<Box<TransactionState>>,
}

impl LazyState {
    fn new() -> Self {
        LazyState { inner: None }
    }

    fn eager(state: TransactionState) -> Self {
        LazyState {
            inner: Some(Box::new(state)),
        }
    }

    pub fn get(&self) -> Option<&TransactionState> {
        self.inner.as_deref()
    }

    pub fn get_mut(&mut self) -> Option<&mut TransactionState> {
        self.inner.as_deref_mut()
    }

    pub fn get_or_init(&mut self, db: Database<'_>) -> &mut TransactionState {
        self.inner.get_or_insert_with(|| {
            let client_id = db.meta().client_id().unwrap();
            let begin_state = db.state_vector().state_vector().unwrap();
            Box::new(TransactionState::new(client_id, begin_state, None))
        })
    }

    pub fn take(&mut self) -> Option<Box<TransactionState>> {
        self.inner.take()
    }

    pub fn origin(&self) -> Option<&Origin> {
        self.inner.as_ref()?.origin.as_ref()
    }
}

pub struct Transaction<'db> {
    pub db: DbHandle<'db>,
    pub state: LazyState,
}

impl<'db> Transaction<'db> {
    pub(crate) fn read_write(
        txn: RwTxn<'db>,
        handle: Dbi,
        client_id: Option<ClientID>,
        origin: Option<Origin>,
    ) -> crate::Result<Self> {
        let db = DbHandle { txn, handle };
        if let Some(client_id) = client_id {
            db.get().meta().insert("client_id", client_id.as_bytes())?;
        }
        let state = match origin {
            None => LazyState::new(),
            Some(origin) => {
                let database = db.get();
                let client_id = database.meta().client_id()?;
                let begin_state = database.state_vector().state_vector()?;
                LazyState::eager(TransactionState::new(client_id, begin_state, Some(origin)))
            }
        };
        Ok(Self { db, state })
    }

    pub fn client_id(&self) -> Option<&ClientID> {
        let state = self.state.get()?;
        Some(&state.client_id)
    }

    pub fn origin(&self) -> Option<&Origin> {
        self.state.origin()
    }

    pub fn state_vector(&self) -> crate::Result<StateVector> {
        if let Some(state) = self.state.get() {
            Ok(state.current_state.clone())
        } else {
            self.db.get().state_vector().state_vector()
        }
    }

    pub fn incremental_update(&self) -> crate::Result<Vec<u8>> {
        todo!()
    }

    pub fn diff_update(&self, since: &StateVector) -> crate::Result<Bytes> {
        let mut buf = BytesMut::new().writer();
        self.diff_update_with(since, &mut buf)?;
        Ok(buf.into_inner().freeze())
    }

    pub fn diff_update_with<W: Write>(
        &self,
        since: &StateVector,
        writer: &mut W,
    ) -> crate::Result<()> {
        let mut writer = EncoderV1::new(writer);
        // wrote updates
        let current_state = self.state_vector()?;
        let db = self.db.get();
        let blocks = db.blocks();
        let mut block_cursor = blocks.cursor()?;
        // in order to build delete set we need to go through all the blocks anyway
        match block_cursor.start_from(ID::new(1.into(), 0.into())) {
            Ok(_) => {}
            Err(crate::Error::NotFound) => {
                // no blocks to encode
                writer.write_var(0usize)?;
                IDSet::default().encode_with(&mut writer)?;
                return Ok(());
            }
            Err(e) => return Err(e),
        }

        let mut current_client = ClientID::ROOT;
        let mut min_state = Clock::new(0);
        let mut max_state = Clock::new(0);

        // we need 2 passes: in the first pass, we go through all the blocks, construct delete set
        // and determine number of blocks we're going to encode (required by lib0 v1 encoding)
        let mut blocks = BTreeMap::new();
        let mut ds = IDSet::default();
        let mut client_block_count = 0;
        let mut first_block_clock = Clock::new(0);
        let mut current = block_cursor.current().optional()?;
        while let Some(block) = current.take() {
            let id = block.id();
            let len = block.clock_len();

            // we moved to blocks in the next client, we need to update range
            if current_client != id.client {
                if client_block_count != 0 {
                    blocks.insert(current_client, (client_block_count, first_block_clock));
                    client_block_count = 0;
                    first_block_clock = Clock::new(0);
                }

                current_client = id.client;
                min_state = since.get(&current_client);
                max_state = current_state.get(&current_client);
            }

            if block.is_deleted() {
                ds.insert(*id, len);
            }

            // check if block overlaps with the range we're interested in
            if id.clock <= max_state && id.clock + len > min_state {
                if client_block_count == 0 {
                    first_block_clock = id.clock;
                }
                client_block_count += 1;
            }

            // move to next block
            current = block_cursor.next()?;
        }

        if client_block_count != 0 {
            blocks.insert(current_client, (client_block_count, first_block_clock));
        }

        // on the second pass we go through blocks we're going to serialize
        // we don't cache them in memory as we don't know how much memory can we spare and how big
        // the document is. Hopefully LMDB will be able to cache it all.
        let contents = db.contents();
        let map_entries = db.map_entries();
        let intern_strings = db.intern_strings();

        writer.write_var(blocks.len())?;
        for (client_id, (block_count, first_clock)) in blocks {
            writer.write_var(block_count)?;
            writer.write_client(client_id)?;

            let block = block_cursor.seek(ID::new(client_id, first_clock))?;
            let clock = since.get(&client_id).max(block.id().clock);
            writer.write_var(clock)?;
            // write first block
            Self::write_block(
                &block,
                clock - block.id().clock,
                &contents,
                &map_entries,
                &intern_strings,
                &mut writer,
            )?;
            // write rest of the blocks
            for _ in 1..block_count {
                match block_cursor.next()? {
                    None => break,
                    Some(block) => Self::write_block(
                        &block,
                        Clock::new(0),
                        &contents,
                        &map_entries,
                        &intern_strings,
                        &mut writer,
                    )?,
                }
            }
        }

        // write delete set
        ds.encode_with(&mut writer)?;

        Ok(())
    }

    fn write_block<E: Encoder>(
        block: &Block<'_>,
        offset: Clock,
        content_store: &ContentStore<'_>,
        map_entries: &MapEntriesStore<'_>,
        strings: &InternStringsStore<'_>,
        writer: &mut E,
    ) -> crate::Result<()> {
        let id = block.id();
        let origin_left = if offset > Clock::new(0) {
            Some(ID::new(id.client, id.clock + offset + 1))
        } else {
            block.origin_left().copied()
        };
        let origin_right = block.origin_right().copied();
        let info = block.info_flags();
        writer.write_info(info)?;
        if let Some(origin_left) = &origin_left {
            writer.write_left_id(origin_left)?;
        }
        if let Some(origin_right) = &origin_right {
            writer.write_right_id(origin_right)?;
        }
        if info & 0b1100_0000 == 0 {
            // left/right origins were not provided
            let parent_id = *block.parent();
            if parent_id.is_root() {
                let parent_name = strings.get(parent_id.clock)?;
                writer.write_parent_info(true)?;
                writer.write_string(parent_name)?;
            } else {
                writer.write_parent_info(false)?;
                writer.write_left_id(&parent_id)?;
            }
            if let Some(&key_hash) = block.key_hash() {
                let entry_key = Self::entry_key_for(map_entries, parent_id, key_hash, block.id())?;
                writer.write_string(entry_key)?;
            }
        }

        let content_type = block.content_type();
        let data = block.try_inline_data();
        match content_type {
            ContentType::Deleted => {
                writer.write_len(block.clock_len().into())?;
            }
            ContentType::Binary => {
                let content = match data {
                    Some(data) => data,
                    None => content_store.get(*block.id())?,
                };
                writer.write_bytes(content)?;
            }
            ContentType::String => {
                let content = match data {
                    Some(data) => data,
                    None => content_store.get(*block.id())?,
                };
                let content = unsafe { std::str::from_utf8_unchecked(content) };
                writer.write_string(content)?;
            }
            ContentType::Embed => {
                let content = match data {
                    Some(data) => data,
                    None => content_store.get(*block.id())?,
                };
                let json: serde_json::Value = serde_json::from_slice(content)?;
                writer.write_json(&json)?;
            }
            ContentType::Format => {
                let content = match data {
                    Some(data) => data,
                    None => content_store.get(*block.id())?,
                };
                writer.write_all(content)?; // format is stored in the same shape
            }
            ContentType::Node => {
                writer.write_type_ref(*block.node_type().unwrap() as u8)?;
            }
            ContentType::Atom | ContentType::Json => match data {
                Some(data) => {
                    writer.write_len(1.into())?;
                    writer.write_all(data)?;
                }
                None => {
                    let mut i = content_store.read_range(content_type, block.range());
                    writer.write_len(block.clock_len().into())?;
                    while let Some(content) = i.next()? {
                        writer.write_all(content.bytes())?;
                    }
                }
            },
            ContentType::Doc => {
                todo!()
            }
        }

        Ok(())
    }

    fn entry_key_for<'a>(
        map_entries: &MapEntriesStore<'a>,
        parent_id: NodeID,
        key_hash: U32,
        block_id: &ID,
    ) -> crate::Result<&'a str> {
        let mut i = map_entries.keys_for_hash(parent_id, key_hash);
        let mut found = None;
        while let Some((key, id)) = i.next()? {
            found = Some(key);
            if id == block_id {
                break;
            }
        }
        found.ok_or_else(|| crate::Error::NotFound)
    }

    fn write_updates(
        cursor: &mut impl Iterator<Item = crate::Result<crate::block::InsertBlockData>>,
        buf: &mut BytesMut,
    ) -> crate::Result<usize> {
        let mut blocks_count = 0;
        for result in cursor {
            let insert = result?;
            blocks_count += 1;
            buf.extend_from_slice(insert.block.as_bytes());
        }
        Ok(blocks_count)
    }

    pub fn apply_update<D: Decoder>(&mut self, decoder: &mut D) -> crate::Result<()> {
        let mut update = Update::decode_with(decoder)?;
        let mut db = self.db.get();
        let state = self.state.get_or_init(db);
        let mut missing_sv = StateVector::default();
        let mut remaining = BTreeMap::new();
        let mut stack = Vec::new();

        if !update.blocks.is_empty() {
            let mut current_client = update.blocks.last_entry().unwrap();
            let mut stack_head = current_client.get_mut().pop_front();

            while let Some(carrier) = stack_head {
                if !carrier.is_skip() {
                    let id = *carrier.id();
                    if state.current_state.contains(&id) {
                        // offset informs if current block partially overlaps with already integrated blocks
                        let offset = state.current_state.get(&id.client) - id.clock;
                        if let Some(dep) = Self::missing_dependency(&carrier, &state.current_state)
                        {
                            // current block is missing a dependency
                            stack.push(carrier);
                            match update.blocks.entry(dep) {
                                Entry::Occupied(e) if !e.get().is_empty() => {
                                    // integrate blocks from the missing dependency client before continuing with the current client
                                    current_client = e;
                                    stack_head = current_client.get_mut().pop_front();
                                    continue;
                                }
                                _ => {
                                    // This update message causally depends on another update message that doesn't exist yet
                                    missing_sv.set_min(dep, state.current_state.get(&dep));
                                    Self::unapplicable(
                                        &mut stack,
                                        &mut update.blocks,
                                        &mut remaining,
                                    );
                                    current_client = update.blocks.last_entry().unwrap();
                                }
                            }
                        } else if offset == 0 || offset < carrier.len() {
                            carrier.integrate(offset, state, &mut db)?;
                        }
                    } else {
                        // update from the same client is missing
                        missing_sv.set_min(id.client, id.clock - 1);
                        stack.push(carrier);
                        Self::unapplicable(&mut stack, &mut update.blocks, &mut remaining);
                        current_client = update.blocks.last_entry().unwrap();
                    }
                }

                // move to the next stack head
                if !stack.is_empty() {
                    stack_head = stack.pop();
                } else {
                    if current_client.get().is_empty() {
                        current_client.remove();
                        current_client = match update.blocks.last_entry() {
                            Some(e) => e,
                            None => break,
                        };
                        stack_head = current_client.get_mut().pop_front();
                    } else {
                        stack_head = current_client.get_mut().pop_front();
                    }
                }
            }
        }
        let pending_delete_set = self.apply_delete(&update.delete_set)?;
        if !remaining.is_empty() || !pending_delete_set.is_empty() {
            todo!("insert pending data")
        }
        Ok(())
    }

    fn apply_delete(&mut self, delete_set: &IDSet) -> crate::Result<IDSet> {
        let mut unapplied = IDSet::default();
        if delete_set.is_empty() {
            return Ok(unapplied);
        }
        let db = self.db.get();
        let state = self.state.get_or_init(db);
        // We can ignore the case of GC and Delete structs, because we are going to skip them
        let blocks = db.blocks();
        let mut block_cursor = blocks.cursor()?;
        for (&client, ranges) in delete_set.iter() {
            let current_clock = state.current_state.get(&client);

            for range in ranges.iter() {
                let clock_start = range.start;
                let clock_end = range.end;
                if clock_start < current_clock {
                    // range exists within already integrated blocks
                    if current_clock < clock_end {
                        unapplied.insert(ID::new(client, clock_start), clock_end - current_clock);
                    }

                    // We can ignore the case of GC and Delete structs, because we are going to skip them
                    block_cursor.start_from(ID::new(client, clock_start))?;
                    if let Some(mut block) = block_cursor.current().optional()? {
                        if block.id().client != client {
                            continue; // we shoot over the current client range
                        }

                        if !block.is_deleted() && block.id().clock < clock_start {
                            // split the first item if necessary
                            let offset = clock_start - block.id().clock;
                            let mut left: BlockMut = block.clone().into();
                            if let Some(right) = left.split(offset) {
                                block_cursor.update_current(left.header())?;
                                block_cursor.insert(right.as_block())?;

                                // block is the same as right, but we need specifically its reference residing in the db
                                block = block_cursor.current()?;
                            }
                        }

                        while block.id().client == client && block.id().clock < clock_end {
                            if !block.is_deleted() {
                                if block.id().clock + block.clock_len() > clock_end {
                                    let offset = clock_end - block.id().clock;
                                    let mut left: BlockMut = block.clone().into();
                                    if let Some(right) = left.split(offset) {
                                        block_cursor.update_current(left.header())?;
                                        block_cursor.insert(right.as_block())?;
                                        block = block_cursor.prev()?.unwrap();
                                    }
                                }
                                let mut block: BlockMut = block.into();
                                block.set_deleted();
                                block_cursor.update_current(block.header())?;
                                state.delete_set.insert(*block.id(), block.clock_len());
                            }
                            block = match block_cursor.next()? {
                                Some(b) => b,
                                None => break,
                            };
                        }
                    }
                } else {
                    unapplied.insert(ID::new(client, range.start), range.end - range.start);
                }
            }
        }
        Ok(unapplied)
    }

    /// Push all pending blocks with the same client ID as `block` into the database.
    /// These blocks are not immediately integrated, since they are missing dependencies on other blocks.
    fn unapplicable(
        stack: &mut Vec<Carrier>,
        blocks: &mut BTreeMap<ClientID, VecDeque<Carrier>>,
        remaining: &mut BTreeMap<ClientID, VecDeque<Carrier>>,
    ) {
        for carrier in stack.drain(..) {
            let client = carrier.id().client;
            if let Some(mut unapplicable) = blocks.remove(&client) {
                // decrement because we weren't able to apply previous operation
                unapplicable.push_front(carrier);
                remaining.insert(client, unapplicable);
            } else {
                // item was the last item on clientsStructRefs and the field was already cleared.
                // Add item to restStructs and continue
                remaining.insert(client, VecDeque::from([carrier]));
            }
        }
    }

    /// Check if current `block` has any missing dependencies on other blocks that are not yet integrated.
    /// A dependency is missing if any of the block's origins (left, right, parent) point to a block that is not yet integrated.
    /// Returns the client ID of the missing dependency, or None if all dependencies are satisfied.
    fn missing_dependency(block: &Carrier, local_sv: &StateVector) -> Option<ClientID> {
        if let Carrier::Block(insert) = block {
            if let Some(origin) = &insert.block.origin_left() {
                if origin.client != insert.id().client
                    && origin.clock >= local_sv.get(&origin.client)
                {
                    return Some(origin.client);
                }
            }

            if let Some(right_origin) = &insert.block.origin_right() {
                if right_origin.client != insert.id().client
                    && right_origin.clock >= local_sv.get(&right_origin.client)
                {
                    return Some(right_origin.client);
                }
            }

            if let Some(Node::Nested(parent)) = insert.parent() {
                if parent.client != insert.id().client
                    && parent.clock >= local_sv.get(&parent.client)
                {
                    return Some(parent.client);
                }
            }
        }

        None
    }

    pub fn commit(mut self, summary: Option<&mut TransactionSummary>) -> crate::Result<()> {
        if let Some(mut state) = self.state.take() {
            let db = self.db.get();
            state.precommit(db, summary)?;
        }
        self.db.commit()
    }

    pub fn snapshot(&self) -> crate::Result<Snapshot> {
        todo!()
    }
}

#[derive(Debug, Default, Clone)]
pub struct TransactionSummary {
    flags: CommitFlags,
    update: Bytes,
    changed_nodes: HashSet<NodeID>,
}

impl TransactionSummary {
    pub fn new(flags: CommitFlags) -> Self {
        Self {
            flags,
            update: Bytes::default(),
            changed_nodes: HashSet::new(),
        }
    }

    #[inline]
    pub fn flags(&self) -> CommitFlags {
        self.flags
    }

    pub fn clear(&mut self) {
        self.update.clear();
        self.changed_nodes.clear();
    }

    pub fn update(&self) -> &Bytes {
        &self.update
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommitFlags(u8);

bitflags! {
    impl CommitFlags : u8 {
        const NONE = 0b0000_0000;
        const UPDATE_V1 = 0b0000_0001;
        const UPDATE_V2 = 0b0000_0010;
        const OBSERVE_NODES = 0b0000_0100;
        const OBSERVE_NODES_DEEP = 0b0000_1000;
    }
}

impl Default for CommitFlags {
    fn default() -> Self {
        CommitFlags::NONE
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Origin(Box<[u8]>);

impl Origin {
    pub fn new(data: &[u8]) -> Self {
        Self(data.into())
    }
}

impl AsRef<[u8]> for Origin {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<'a, T> From<&'a T> for Origin
where
    T: AsRef<[u8]>,
{
    fn from(value: &'a T) -> Self {
        Origin(value.as_ref().into())
    }
}

impl Display for Origin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match std::str::from_utf8(&self.0) {
            // for strings try to print them as utf8
            Ok(s) => write!(f, "{}", s),
            _ => {
                // for non-strings print as hex
                for byte in &self.0 {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
        }
    }
}
