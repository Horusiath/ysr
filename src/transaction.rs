use crate::block::{Block, BlockMut, ID};
use crate::block_reader::{Carrier, Update};
use crate::content::{ContentFormat, ContentType};
use crate::id_set::IDSet;
use crate::node::{Node, NodeID};
use crate::read::Decoder;
use crate::state_vector::Snapshot;
use crate::store::lmdb::store::{
    BlockContentKey, BlockKey, CursorExt, MapBucketHashKey, KEY_PREFIX_BLOCK, KEY_PREFIX_INTERN_STR,
};
use crate::store::lmdb::BlockStore;
use crate::write::{Encode, Encoder, EncoderV1, WriteExt};
use crate::{ClientID, Clock, Optional, StateVector, U32};
use bitflags::bitflags;
use bytes::{BufMut, Bytes, BytesMut};
use lmdb_rs_m::{Database, DbHandle};
use smallvec::{smallvec, SmallVec};
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

    pub fn next_id(&mut self) -> ID {
        let clock = self.current_state.inc_by(self.client_id, Clock::new(1));
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

    pub(crate) fn delete(
        &mut self,
        db: &mut Database,
        block: &mut BlockMut,
        parent_deleted: bool,
    ) -> crate::Result<bool> {
        let mut cursor = db.new_cursor()?;
        cursor.to_key(&BlockKey::new(*block.id()))?;
        cursor.delete_current(self, block, parent_deleted)
    }

    fn precommit<'db>(
        &mut self,
        db: Database<'_>,
        summary: Option<&mut TransactionSummary>,
    ) -> crate::Result<()> {
        // squash delete set
        self.delete_set.squash();

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
        let mut cursor = db.new_cursor()?;
        let mut key_changes = BTreeMap::new();
        for (client, &clock) in self.current_state.iter() {
            let before_clock = self.begin_state.get(client);
            if before_clock != clock {
                let key = BlockKey::new(ID::new(*client, clock));
                cursor.to_gte_key(&key)?;
                Self::merge_with_lefts(&mut cursor, &mut key_changes)?;
            }
        }

        // try to merge mergeStructs

        // create incremental update

        //TODO: subdoc events

        Ok(())
    }

    /// Moving cursor right to left, try to merge structs with their left neighbors.
    /// Returns ID of the current position after merging.
    /// Expects that cursor is set within a block keyspace position.
    fn merge_with_lefts(
        cursor: &mut lmdb_rs_m::Cursor,
        key_changes: &mut BTreeMap<(NodeID, U32, ID), ID>,
    ) -> crate::Result<ID> {
        let mut right: BlockMut = cursor.get_block()?.into();
        cursor.to_prev_key()?;
        let mut left = cursor.get_block().optional()?.map(BlockMut::from);
        while let Some(curr) = &mut left {
            if curr.merge(right.as_block()) {
                if let Some(&parent_sub) = right.key_hash() {
                    let e = key_changes
                        .entry((*right.parent(), parent_sub, *right.id()))
                        .or_insert(*curr.id());
                    *e = *curr.id();
                }
                cursor.del()?;
            } else {
                break; // we couldn't merge left and right blocks
            }

            // move to left block
            cursor.to_prev_key()?;
            right = std::mem::replace(
                &mut left,
                cursor.get_block().optional()?.map(BlockMut::from),
            )
            .unwrap();
        }

        Ok(*right.id())
    }
}

pub struct Transaction<'db> {
    txn: lmdb_rs_m::Transaction<'db>,
    client_id: ClientID,
    handle: DbHandle,
    state: Option<Box<TransactionState>>,
}

impl<'db> Transaction<'db> {
    pub(crate) fn read_write(
        txn: lmdb_rs_m::Transaction<'db>,
        handle: DbHandle,
        origin: Option<Origin>,
        client_id: ClientID,
    ) -> Self {
        let state = origin.map(|o| {
            let db = txn.bind(&handle);
            let begin_state = db.state_vector().unwrap();
            Box::new(TransactionState::new(client_id, begin_state, Some(o)))
        });
        Self {
            txn,
            client_id,
            handle,
            state,
        }
    }

    pub fn db(&self) -> Database<'_> {
        self.txn.bind(&self.handle)
    }

    pub fn origin(&self) -> Option<&Origin> {
        let state = self.state.as_ref()?;
        state.origin.as_ref()
    }

    pub fn split_mut(&mut self) -> (Database<'_>, &mut TransactionState) {
        let db = self.txn.bind(&self.handle);
        let state = self.state.get_or_insert_with(|| {
            let begin_state = db.state_vector().unwrap();
            Box::new(TransactionState::new(self.client_id, begin_state, None))
        });
        (db, state)
    }

    pub fn state_vector(&self) -> crate::Result<StateVector> {
        self.db().state_vector()
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
        let db = self.db();
        let mut block_cursor = db.new_cursor()?;
        // in order to build delete set we need to go through all the blocks anyway
        if block_cursor
            .to_gte_key(&[KEY_PREFIX_BLOCK].as_slice())
            .optional()? // MdbError::NotFound - there are no blocks
            .is_none()
        {
            writer.write_var(0)?; // no blocks
            writer.write_var(0)?; // no elements in delete set
            return Ok(());
        }

        let mut current_client = ClientID::MAX_VALUE;
        let mut min_state = Clock::new(0);
        let mut max_state = Clock::new(0);

        // we need 2 passes: in the first pass, we go through all the blocks, construct delete set
        // and determine number of blocks we're going to encode (required by lib0 v1 encoding)
        let mut blocks = BTreeMap::new();
        let mut ds = IDSet::default();
        let mut client_block_count = 0;
        let mut first_block_clock = Clock::new(0);
        while let Some(block) = block_cursor.get_block().optional()? {
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
            if block_cursor.to_next_key().optional()?.is_none() {
                break;
            }
        }

        if client_block_count != 0 {
            blocks.insert(current_client, (client_block_count, first_block_clock));
        }

        // on the second pass we go through blocks we're going to serialize
        // we don't cache them in memory as we don't know how much memory can we spare and how big
        // the document is. Hopefully LMDB will be able to cache it all.
        let mut content_cursor = db.new_cursor()?;

        writer.write_var(blocks.len())?;
        for (client_id, (block_count, first_clock)) in blocks {
            writer.write_var(block_count)?;
            writer.write_client(client_id)?;

            block_cursor.to_key(&BlockKey::new(ID::new(client_id, first_clock)))?;
            let block = block_cursor.get_block()?;
            let clock = since.get(&client_id).max(block.id().clock);
            writer.write_var(clock)?;
            // write first block
            Self::write_block(
                &block,
                clock - block.id().clock,
                &mut content_cursor,
                &mut writer,
            )?;
            // write rest of the blocks
            for _ in 1..block_count {
                block_cursor.to_next_key()?;
                let block = block_cursor.get_block()?;
                Self::write_block(&block, Clock::new(0), &mut content_cursor, &mut writer)?;
            }
        }

        // write delete set
        ds.encode_with(&mut writer)?;

        Ok(())
    }

    fn write_block<E: Encoder>(
        block: &Block<'_>,
        offset: Clock,
        cursor: &mut lmdb_rs_m::Cursor<'_>,
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
            let parent_id = block.parent();
            if parent_id.is_root() {
                let parent_name = Self::parent_name(cursor, parent_id)?;
                writer.write_parent_info(true)?;
                writer.write_string(parent_name)?;
            } else {
                writer.write_parent_info(false)?;
                writer.write_left_id(parent_id)?;
            }
            if let Some(key_hash) = block.key_hash() {
                let entry_key = Self::entry_key_for(cursor, *parent_id, *key_hash, block.id())?;
                writer.write_string(entry_key)?;
            }
        }

        let content_type = block.content_type();
        match content_type {
            ContentType::Deleted => {
                writer.write_len(block.clock_len().into())?;
            }
            ContentType::Binary => {
                let content = cursor.content(*block.id())?;
                writer.write_bytes(content)?;
            }
            ContentType::String => {
                let content = cursor.content(*block.id())?;
                let content = unsafe { std::str::from_utf8_unchecked(content) };
                writer.write_string(content)?;
            }
            ContentType::Embed => {
                let content = cursor.content(*block.id())?;
                let json: serde_json::Value = serde_json::from_slice(content)?;
                writer.write_json(&json)?;
            }
            ContentType::Format => {
                let content = cursor.content(*block.id())?;
                writer.write_all(content)?; // format is stored in the same shape
            }
            ContentType::Node => {
                writer.write_type_ref(*block.node_type().unwrap() as u8)?;
            }
            ContentType::Atom | ContentType::Json => {
                writer.write_len(block.clock_len().into())?;
                cursor.to_key(&BlockContentKey::new(*block.id()))?;
                for _ in 0..block.clock_len().get() {
                    let value: &[u8] = cursor.get_value()?;
                    writer.write_all(&value[1..])?; // skip 1st byte (content_type)
                    cursor.to_next_key()?;
                }
            }
            ContentType::Doc => {
                let content = cursor.content(*block.id())?;
                todo!()
            }
        }

        Ok(())
    }

    fn entry_key_for<'a>(
        cursor: &mut lmdb_rs_m::Cursor<'a>,
        parent_id: NodeID,
        key_hash: U32,
        block_id: &ID,
    ) -> crate::Result<&'a str> {
        let bucket_key = MapBucketHashKey::new(parent_id, key_hash);
        cursor.to_gte_key(&bucket_key)?;
        while {
            let key: &[u8] = cursor.get_key()?;
            if key.starts_with(bucket_key.as_bytes()) {
                let value = ID::parse(cursor.get_value()?)?;
                if value == block_id {
                    let entry_key = unsafe {
                        std::str::from_utf8_unchecked(&key[size_of::<MapBucketHashKey>()..])
                    };
                    return Ok(entry_key);
                }
                true
            } else {
                false
            }
        } {}

        Err(crate::Error::NotFound)
    }

    fn parent_name<'a>(cursor: &mut lmdb_rs_m::Cursor<'a>, id: &NodeID) -> crate::Result<&'a str> {
        let mut key: SmallVec<[u8; 5]> = smallvec![KEY_PREFIX_INTERN_STR];
        key.extend_from_slice(id.clock.as_bytes());
        cursor.to_key(&key.as_bytes())?;
        let str = unsafe { std::str::from_utf8_unchecked(cursor.get_value()?) };
        Ok(str)
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
        let (mut db, state) = self.split_mut();
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
            self.db()
                .insert_pending_update(&missing_sv, &remaining, &pending_delete_set)?;
        }
        Ok(())
    }

    fn apply_delete(&mut self, delete_set: &IDSet) -> crate::Result<IDSet> {
        let mut unapplied = IDSet::default();
        if delete_set.is_empty() {
            return Ok(unapplied);
        }
        let (mut db, state) = self.split_mut();
        // We can ignore the case of GC and Delete structs, because we are going to skip them
        let mut cursor = db.new_cursor()?;
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
                    cursor.to_gte_key(&BlockKey::new(ID::new(client, clock_start)))?;
                    if let Some(mut block) = cursor.get_block().optional()? {
                        if block.id().client != client {
                            continue; // we shoot over the current client range
                        }

                        if !block.is_deleted() && block.id().clock < clock_start {
                            // split the first item if necessary
                            let offset = clock_start - block.id().clock;
                            let mut left: BlockMut = block.clone().into();
                            if let Some(right) = left.split(offset) {
                                cursor.replace(&left.header().as_bytes())?;
                                cursor.set(
                                    &BlockKey::new(*right.id()),
                                    &right.header().as_bytes(),
                                    0,
                                )?;
                                block = cursor.get_block()?;
                            }
                        }

                        while block.id().client == client && block.id().clock < clock_end {
                            if !block.is_deleted() {
                                if block.id().clock + block.clock_len() > clock_end {
                                    let offset = clock_end - block.id().clock;
                                    let mut left: BlockMut = block.clone().into();
                                    if let Some(right) = left.split(offset) {
                                        cursor.replace(&left.header().as_bytes())?;
                                        cursor.set(
                                            &BlockKey::new(*right.id()),
                                            &right.header().as_bytes(),
                                            0,
                                        )?;
                                        cursor.to_prev_key()?;
                                        block = cursor.get_block()?;
                                    }
                                }
                                let mut block: BlockMut = block.into();
                                cursor.delete_current(state, &mut block, false)?;
                            }
                            cursor.to_next_key()?;
                            block = match cursor.get_block().optional()? {
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
            // commit the transaction
            state.precommit(self.db(), summary)?;
            self.txn.commit()?;
        }
        Ok(())
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
