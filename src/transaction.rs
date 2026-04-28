use crate::block::{Block, BlockMut, ID};
use crate::block_reader::{Carrier, Update};
use crate::content::{ContentType, FormatAttribute};
use crate::gc::GarbageCollector;
use crate::id_set::IDSet;
use crate::lib0::v1::{DecoderV1, EncoderV1};
use crate::lib0::v2::{DecoderV2, EncoderV2};
use crate::lib0::{Decode, Decoder, Encode, Encoder, Encoding, WriteExt};
use crate::lmdb::{Database, Dbi, RwTxn};
use crate::node::{Node, NodeID};
use crate::state_vector::Snapshot;
use crate::store::block_store::BlockCursor;
use crate::store::content_store::ContentStore;
use crate::store::intern_strings::InternStringsStore;
use crate::store::meta_store::MetaStore;
use crate::store::{Db, MapEntriesStore};
use crate::{ClientID, Clock, Error, Optional, StateVector, U32, lib0};
use bitflags::bitflags;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::ops::{Deref, DerefMut};
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

    /// Checks if item with a given `id` has been added to a block store within this transaction.
    pub fn has_added(&self, id: &ID) -> bool {
        id.clock >= self.begin_state.get(&id.client)
    }

    /// Checks if item with a given `id` has been deleted within this transaction.
    pub fn has_deleted(&self, id: &ID) -> bool {
        self.delete_set.contains(id)
    }

    fn precommit(
        &mut self,
        db: Database<'_>,
        mut summary: Option<&mut TransactionSummary>,
    ) -> crate::Result<()> {
        // squash delete set
        self.delete_set.squash();
        let blocks = db.blocks();

        // transaction.afterState = getStateVector(transaction.doc.store)

        if let Some(summary) = summary.as_deref_mut()
            && summary.flags.contains(CommitFlags::OBSERVE_NODES)
        {
            summary.changed_nodes.extend(self.changed.keys());
            // todo!();
            // if summary.flags.contains(CommitFlags::OBSERVE_NODES_DEEP) {
            //     // bubble up changes to parent nodes and gather them as well
            //     todo!();
            // }
        }

        //TODO: if (doc.gc) {
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
        for id in self.merge_blocks.iter() {
            if let Some(_) = cursor.seek_containing(*id).optional()? {
                Self::merge_with_lefts(&mut cursor, &mut key_changes)?;
            }
        }

        // persist updated state vector
        let mut sv_store = db.state_vector();
        for (client, &clock) in self.current_state.iter() {
            sv_store.update(*client, clock)?;
        }

        // create incremental update
        if let Some(summary) = summary
            && (self.begin_state != self.current_state || !self.delete_set.is_empty())
        {
            if summary.flags.contains(CommitFlags::UPDATE_V1) {
                let mut encoder = EncoderV1::new(&mut summary.update);
                self.incremental_update(&db, &mut encoder)?;
            } else if summary.flags.contains(CommitFlags::UPDATE_V2) {
                let mut encoder = EncoderV2::new(&mut summary.update);
                self.incremental_update(&db, &mut encoder)?;
            }
        }

        //TODO: subdoc events

        Ok(())
    }

    fn incremental_update<E: Encoder>(
        &self,
        db: &Database<'_>,
        writer: &mut E,
    ) -> crate::Result<()> {
        /*
           The write path works as follows:
           - {varint} number of clients affected
             for each client:
               - {varint} number of blocks to encode sharing the same client ID
               - {varint} client ID
               - {varint} clock describing start of the consecutive block range
               for each block starting from the clock - encode block itself:
                  - {u8} block info flags (content type, parent kind, neighbor presence)
                  - (optional) {ID} block origin left
                  - (optional) {ID} block origin right
                  - (optional) {ID|string} block parent
                  - (optional) {string} block entry key (if block is a map entry)
                  - block content
           - delete set: {varint} number of clients in delete set
               for each client in delete set:
               - {varint} client ID
               - {varint} number of ranges to encode
               for each range in ranges:
                   - {varint} first clock of the delete range
                   - {varint} length (number of consecutive deleted elements)
        */
        let begin_state = &self.begin_state;
        let current_state = &self.current_state;
        // wrote updates
        let blocks = db.blocks();
        let contents = db.contents();
        let intern_strings = db.intern_strings();
        let map_entries = db.map_entries();
        let mut cursor = blocks.cursor()?;

        let changed_state = current_state
            .iter()
            .filter(|(client_id, end_clock)| {
                let start_clock = begin_state.get(client_id);
                end_clock > &&start_clock
            })
            .collect::<Vec<_>>();
        writer.write_var(changed_state.len())?;
        let mut block_buf = Vec::new();
        for (&client_id, &end_clock) in changed_state {
            block_buf.clear();
            let start_clock = begin_state.get(&client_id);

            // for incremental update we can buffer them all
            let mut block = cursor
                .seek_containing(ID::new(client_id, start_clock))
                .optional()?;
            while let Some(current) = block
                && current.id().client == client_id
                && current.last_id().clock <= end_clock
            {
                block_buf.push(current);
                block = cursor.next()?;
            }

            // then we can write the blocks for the same client
            writer.write_var(block_buf.len())?;
            writer.write_client(client_id)?;

            let block = &block_buf[0];
            writer.write_var(start_clock)?;
            // write first block - it may start at offset inside the block
            Transaction::write_block(
                block,
                start_clock - block.id().clock,
                &contents,
                &map_entries,
                &intern_strings,
                writer,
            )?;
            // write rest of the blocks
            for block in block_buf[1..].iter() {
                Transaction::write_block(
                    block,
                    Clock::new(0),
                    &contents,
                    &map_entries,
                    &intern_strings,
                    writer,
                )?
            }
        }

        // write down transaction's own delete set
        self.delete_set.encode_with(writer)?;

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
        let _end = *right.id();
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

    pub(crate) fn get(&self) -> Option<&TransactionState> {
        self.inner.as_deref()
    }

    pub(crate) fn get_or_init(&mut self, db: Database<'_>) -> &mut TransactionState {
        self.inner.get_or_insert_with(|| {
            let client_id = db.meta().client_id().unwrap();
            let begin_state = db.state_vector().state_vector().unwrap();
            Box::new(TransactionState::new(client_id, begin_state, None))
        })
    }

    pub(crate) fn take(&mut self) -> Option<Box<TransactionState>> {
        self.inner.take()
    }

    /// Returns an origin passed to this transaction when it was created
    /// with [crate::MultiDoc::transact_mut_with].
    pub fn origin(&self) -> Option<&Origin> {
        self.inner.as_ref()?.origin.as_ref()
    }
}

pub struct Transaction<'db> {
    pub db: DbHandle<'db>,
    pub state: LazyState,
}

impl<'db> Transaction<'db> {
    pub(crate) fn read_only(txn: RwTxn<'db>, handle: Dbi) -> Self {
        let db = DbHandle { txn, handle };
        Transaction {
            db,
            state: LazyState::new(),
        }
    }

    pub(crate) fn read_write(
        txn: RwTxn<'db>,
        handle: Dbi,
        client_id: Option<ClientID>,
        origin: Option<Origin>,
    ) -> crate::Result<Self> {
        let db = DbHandle { txn, handle };
        if let Some(client_id) = client_id {
            db.get()
                .meta()
                .insert(MetaStore::KEY_CLIENT_ID, client_id.as_bytes())?;
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

    /// Returns a globally unique identifier of the current client.
    pub fn client_id(&self) -> Option<&ClientID> {
        let state = self.state.get()?;
        Some(&state.client_id)
    }

    /// Returns an origin passed to this transaction when it was created
    /// with [crate::MultiDoc::transact_mut_with].
    pub fn origin(&self) -> Option<&Origin> {
        self.state.origin()
    }

    /// Returns a current state vector of this transaction.
    ///
    /// For read-write transactions it includes changes made by current transaction.
    /// For read-only transactions it only shows the changes at the moment when the transaction was
    /// created.
    pub fn state_vector(&self) -> crate::Result<StateVector> {
        if let Some(state) = self.state.get() {
            Ok(state.current_state.clone())
        } else {
            self.db.get().state_vector().state_vector()
        }
    }

    /// Removes all the contents of the document, but keeping the empty document itself.
    /// This doesn't cause the database file to shrink, but it releases the space occupied by this
    /// document to be reused by other documents and their changes.
    pub fn clear_all(&mut self) -> crate::Result<()> {
        self.db.get().clear()?;
        Ok(())
    }

    /// Returns an update which contains only changes made within the scope of this transaction.
    ///
    /// You can also use [Transaction::commit] with a `summary` parameter specified and configured
    /// to use [CommitFlags::UPDATE_V1]/[CommitFlags::UPDATE_V2] to retrieve the update combined
    /// with confirmed commit operation.
    pub fn incremental_update(&self, version: Encoding) -> crate::Result<Vec<u8>> {
        let mut buf = Vec::new();
        match version {
            Encoding::V1 => {
                let mut encoder = EncoderV1::new(&mut buf);
                self.incremental_update_with(&mut encoder)?;
            }
            Encoding::V2 => {
                let mut encoder = EncoderV2::new(&mut buf);
                self.incremental_update_with(&mut encoder)?;
            }
        }
        Ok(buf)
    }

    /// Returns an update that contains all changes that happened `since` a given state vector.
    pub fn diff_update(&self, since: &StateVector, version: Encoding) -> crate::Result<Vec<u8>> {
        let mut buf = Vec::new();
        match version {
            Encoding::V1 => {
                let mut encoder = EncoderV1::new(&mut buf);
                self.diff_update_with(since, &mut encoder)?;
            }
            Encoding::V2 => {
                let mut encoder = EncoderV2::new(&mut buf);
                self.diff_update_with(since, &mut encoder)?;
            }
        }
        Ok(buf)
    }

    pub fn diff_update_with<E: Encoder>(
        &self,
        since: &StateVector,
        writer: &mut E,
    ) -> crate::Result<()> {
        // wrote updates
        let current_state = self.state_vector()?;
        let db = self.db.get();
        let blocks = db.blocks();
        let mut block_cursor = blocks.cursor()?;
        // in order to build delete set we need to go through all the blocks anyway
        match block_cursor.start_from(ID::new(1.into(), 0.into())) {
            Ok(_) => {}
            Err(Error::NotFound) => {
                // no blocks to encode
                writer.write_var(0usize)?;
                IDSet::default().encode_with(writer)?;
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
                writer,
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
                        writer,
                    )?,
                }
            }
        }

        // write delete set
        ds.encode_with(writer)?;

        Ok(())
    }

    pub fn incremental_update_with<E: Encoder>(&self, writer: &mut E) -> crate::Result<()> {
        if let Some(state) = self.state.get() {
            let db = self.db.get();
            state.incremental_update(&db, writer)?;
        }
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
                writer.write_len(block.clock_len())?;
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
                let value: lib0::Value = lib0::from_slice(content)?;
                writer.write_json(&value)?;
            }
            ContentType::Format => {
                let content = match data {
                    Some(data) => data,
                    None => content_store.get(*block.id())?,
                };
                let fmt =
                    FormatAttribute::new(content).ok_or_else(|| Error::InvalidMapping("format"))?;
                writer.write_key(fmt.key())?;
                writer.write_json(&fmt.value::<lib0::Value>()?)?;
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
                    writer.write_len(block.clock_len())?;
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
        found.ok_or_else(|| Error::NotFound)
    }

    /// Decodes an incoming `update` (which will be decoded using provided lib0 `version`) and
    /// integrates the changes it provided into current document.
    ///
    /// Any missing updates that would block the changes from being integrated will be stashed
    /// (and persisted) aside as pending updates (you can access them using [MetaStore::pending]
    /// method).
    pub fn apply_update(&mut self, update: &[u8], version: Encoding) -> crate::Result<()> {
        match version {
            Encoding::V1 => self.apply_update_with(&mut DecoderV1::from_slice(update)),
            Encoding::V2 => self.apply_update_with(&mut DecoderV2::from_slice(update)?),
        }
    }

    pub fn apply_update_with<D: Decoder>(&mut self, decoder: &mut D) -> crate::Result<()> {
        let mut current = Some(Update::decode_with(decoder)?);
        while let Some(update) = current.take() {
            let mut tx = self.write_context()?;
            let remaining = if !update.blocks.is_empty() {
                tx.apply_update_internal(update.blocks)?
            } else {
                BTreeMap::default()
            };
            let pending_delete_set = tx.apply_delete(&update.delete_set)?;
            drop(tx);

            current = self.handle_pending(Update {
                blocks: remaining,
                delete_set: pending_delete_set,
            })?;
        }
        Ok(())
    }

    fn handle_pending(&mut self, update: Update) -> crate::Result<Option<Update>> {
        let db = self.db.get();
        let meta = db.meta();
        let pending = meta.pending()?;

        if pending.is_none() && update.blocks.is_empty() && update.delete_set.is_empty() {
            return Ok(None);
        }

        let state = self.state.get_or_init(db);

        let mut retry = false;
        let mut pending = match pending {
            None => PendingUpdate::default(),
            Some(pending) => {
                for (client, clock) in pending.missing_sv.iter() {
                    if clock < &state.current_state.get(client) {
                        retry = true;
                        break;
                    }
                }
                pending
            }
        };
        if !update.blocks.is_empty() {
            for (client, blocks) in update.blocks.iter() {
                if let Some(first) = blocks.front() {
                    pending
                        .missing_sv
                        .set_min(*client, first.id().clock - Clock::new(1));
                }
            }
        }

        let mut pending_update = if pending.update.is_empty() {
            Update::default()
        } else {
            Update::decode(pending.update, Encoding::V1)?
        };
        if !pending.delete_set.is_empty() {
            pending_update.delete_set = IDSet::decode(pending.delete_set, Encoding::V1)?;
        }

        let missing_sv = pending.missing_sv;
        let pending = Update::merge_updates(pending_update, update);
        if retry {
            meta.clear_pending()?;
            Ok(Some(pending))
        } else {
            Self::insert_pending(&meta, pending, missing_sv)?;
            Ok(None)
        }
    }

    fn insert_pending(
        meta: &MetaStore,
        update: Update,
        missing_sv: StateVector,
    ) -> crate::Result<()> {
        let mut buf = Vec::new();
        let mut writer = EncoderV1::new(&mut buf);

        writer.write_var(update.blocks.len())?;
        for (&client_id, carriers) in update.blocks.iter() {
            writer.write_var(carriers.len())?;
            writer.write_client(client_id)?;
            writer.write_var(carriers[0].id().clock)?;

            for carrier in carriers {
                carrier.encode(&mut writer)?;
            }
        }
        buf.write_var(0)?; // assume empty delete set (we'll provide it separately)

        let mut ds = Vec::new();
        let mut writer = EncoderV1::new(&mut ds);
        update.delete_set.encode_with(&mut writer)?;
        meta.insert_pending(&PendingUpdate::new(&buf, &ds, missing_sv))?;
        Ok(())
    }

    /// Commits current transaction, optionally filling the transaction `summary` report.
    /// This commit operation also commits underlying database transaction, making changes visible
    /// and permanent.
    ///
    /// Transaction summary can hold specific flags to inform which notifications are we interested
    /// in:
    /// - [CommitFlags::UPDATE_V1] and [CommitFlags::UPDATE_V2] will cause transaction to submit
    ///   the update containing the changes made by this transaction (it can also be obtained before
    ///   commit via [Transaction::incremental_update], but that update may be larger).
    /// - [CommitFlags::OBSERVE_NODES] will include [NodeID] of all the nodes modified as part of
    ///   this transaction.
    pub fn commit(mut self, summary: Option<&mut TransactionSummary>) -> crate::Result<()> {
        if let Some(mut state) = self.state.take() {
            let db = self.db.get();
            state.precommit(db, summary)?;
        }
        self.db.commit()
    }

    /// Returns a snapshot representing a committed state.
    pub fn snapshot_committed(&self) -> crate::Result<Snapshot> {
        let db = self.db.get();
        let sv = db.state_vector().state_vector()?;
        let blocks = db.blocks();
        let mut cursor = blocks.cursor()?;
        let ds = cursor.delete_set()?;

        Ok(Snapshot::new(sv, ds))
    }

    /// Returns a snapshot representing both committed state of the document
    /// and changes made by the current transactions.
    pub fn snapshot_uncommitted(&self) -> crate::Result<Snapshot> {
        let sv = self.state_vector()?;
        let db = self.db.get();
        let blocks = db.blocks();
        let mut cursor = blocks.cursor()?;
        let mut ds = cursor.delete_set()?;
        if let Some(state) = self.state.get() {
            ds.merge(state.delete_set.clone());
        }
        Ok(Snapshot::new(sv, ds))
    }

    /// Performs a garbage collection of items marked in the provided `delete_set`. Only unreachable
    /// collections and their children can be collected cleanly from the database.
    ///
    /// Other elements, which still could be referenced elsewhere, will only be tombstoned
    /// (contents removed, but the rudimentary block metadata will still be there).
    pub fn gc(&mut self, delete_set: &IDSet) -> crate::Result<()> {
        let mut gc = GarbageCollector::new(self.write_context()?);
        gc.collect(delete_set)
    }

    pub fn read_context(&self) -> crate::Result<TxScope<'_>> {
        TxScope::new(self)
    }

    pub fn write_context(&mut self) -> crate::Result<TxMutScope<'_>> {
        TxMutScope::new(self)
    }
}

/// Summary of transaction changes.
#[derive(Debug, Default, Clone)]
pub struct TransactionSummary {
    pub flags: CommitFlags,
    pub update: Vec<u8>,
    pub changed_nodes: HashSet<NodeID>,
}

impl TransactionSummary {
    pub fn new(flags: CommitFlags) -> Self {
        Self {
            flags,
            update: Vec::new(),
            changed_nodes: HashSet::new(),
        }
    }

    pub fn clear(&mut self) {
        self.update.clear();
        self.changed_nodes.clear();
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommitFlags(u8);

bitflags! {
    impl CommitFlags : u8 {
        const NONE = 0b0000_0000;

        /// Include update containing changes made by the transaction,
        /// serialized using lib0 V1 encoding.
        const UPDATE_V1 = 0b0000_0001;

        /// Include update containing changes made by the transaction,
        /// serialized using lib0 V2 encoding.
        const UPDATE_V2 = 0b0000_0010;

        /// Include list of identifiers of nodes which have been modified as part of this transaction.
        const OBSERVE_NODES = 0b0000_0100;

        /// Include list of identifiers of nodes which have been modified as part of this transaction,
        /// including parent nodes of those nodes.
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

#[derive(Default)]
pub struct PendingUpdate<'tx> {
    pub update: &'tx [u8],
    pub delete_set: &'tx [u8],
    pub missing_sv: StateVector,
}

impl<'tx> PendingUpdate<'tx> {
    pub fn new(update: &'tx [u8], delete_set: &'tx [u8], missing_sv: StateVector) -> Self {
        PendingUpdate {
            update,
            delete_set,
            missing_sv,
        }
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.update.is_empty() && self.delete_set.is_empty() && self.missing_sv.is_empty()
    }
}

pub struct TxScope<'tx> {
    pub db: Database<'tx>,
    pub cursor: BlockCursor<'tx>,
}

impl<'tx> TxScope<'tx> {
    pub fn new(tx: &'tx Transaction<'_>) -> crate::Result<Self> {
        let db = tx.db.get();
        let cursor = BlockCursor::new(db)?;
        Ok(Self { db, cursor })
    }
}

pub struct TxMutScope<'tx> {
    inner: TxScope<'tx>,
    pub(crate) state: &'tx mut TransactionState,
}

impl<'tx> TxMutScope<'tx> {
    pub fn new(tx: &'tx mut Transaction<'_>) -> crate::Result<Self> {
        let db = tx.db.get();
        let cursor = BlockCursor::new(db)?;
        let state = tx.state.get_or_init(db);
        Ok(Self {
            inner: TxScope { db, cursor },
            state,
        })
    }

    pub(crate) fn delete(
        &mut self,
        block: &mut BlockMut,
        parent_deleted: bool,
    ) -> crate::Result<bool> {
        if block.is_deleted() {
            return Ok(false);
        }
        block.set_deleted();
        self.cursor.update(block.as_block())?;

        self.state.delete_set.insert(*block.id(), block.clock_len());
        self.state
            .add_changed_type(*block.parent(), parent_deleted, block.key_hash());

        match block.content_type() {
            ContentType::Node => {
                // iterate over list values of the node and delete them
                if let Some(start) = block.start() {
                    self.delete_list_members(*start)?;
                }
                //iterate over map entries of the node and delete them
                self.delete_map_members(block.id())?;
            }
            _ => { /* not used */ }
        }
        Ok(true)
    }

    fn delete_list_members(&mut self, start: ID) -> crate::Result<()> {
        let mut current = Some(start);
        while let Some(id) = current {
            let mut block: BlockMut = self.cursor.seek(id)?.into();
            if !self.delete(&mut block, true)?
                && block.id().clock < self.state.begin_state.get(&block.id().client)
            {
                // This will be gc'd later, and we want to merge it if possible
                // We try to merge all deleted items after each transaction,
                // but we have no knowledge about that this needs to be merged
                // since it is not in transaction.rs. Hence, we add it to transaction._mergeStructs
                self.state.merge_blocks.insert(*block.id());
            }
            current = block.right().copied();
        }
        Ok(())
    }

    fn delete_map_members(&mut self, id: &ID) -> crate::Result<()> {
        let mut to_delete = Vec::new();
        {
            let map_entries = self.db.map_entries();
            let mut entries = map_entries.entries(id);
            while let Some(_) = entries.next()? {
                let child_id = *entries.block_id()?;
                to_delete.push(child_id);
            }
        }

        for entry_id in to_delete {
            let mut block: BlockMut = self.cursor.seek(entry_id)?.into();
            let existed_before = entry_id.clock < self.state.begin_state.get(&block.id().client);
            let deleted = self.delete(&mut block, true)?;
            if deleted && existed_before {
                // same as above
                self.state.merge_blocks.insert(entry_id);
            }
        }
        Ok(())
    }

    fn apply_update_internal(
        &mut self,
        mut blocks: BTreeMap<ClientID, VecDeque<Carrier>>,
    ) -> crate::Result<BTreeMap<ClientID, VecDeque<Carrier>>> {
        let mut missing_sv = StateVector::default();
        let mut remaining = BTreeMap::new();
        let mut stack = Vec::new();

        let mut current_client = blocks.last_entry();
        let mut stack_head = match &mut current_client {
            None => return Ok(remaining),
            Some(e) => e.get_mut().pop_front(),
        };

        while let Some(carrier) = stack_head.take() {
            if !carrier.is_skip() {
                let id = *carrier.id();
                if self.state.current_state.contains(&id) {
                    // offset informs if current block partially overlaps with already integrated blocks
                    let offset = self.state.current_state.get(&id.client) - id.clock;
                    if let Some(dep) = Self::missing_dependency(&carrier, &self.state.current_state)
                    {
                        // current block is missing a dependency
                        stack.push(carrier);
                        match blocks.entry(dep) {
                            Entry::Occupied(mut e) if !e.get().is_empty() => {
                                // integrate blocks from the missing dependency client before continuing with the current client
                                stack_head = e.get_mut().pop_front();
                                current_client = Some(e);
                                continue;
                            }
                            _ => {
                                // This update message causally depends on another update message that doesn't exist yet
                                missing_sv.set_min(dep, self.state.current_state.get(&dep));
                                Self::unapplicable(&mut stack, &mut blocks, &mut remaining);
                                current_client = blocks.last_entry();
                            }
                        }
                    } else if offset == 0 || offset < carrier.len() {
                        carrier.integrate(offset, self)?;
                    }
                } else {
                    // update from the same client is missing
                    missing_sv.set_min(id.client, id.clock - 1);
                    stack.push(carrier);
                    Self::unapplicable(&mut stack, &mut blocks, &mut remaining);
                    current_client = blocks.last_entry();
                }
            }

            // move to the next stack head
            if !stack.is_empty() {
                stack_head = stack.pop();
            } else if let Some(mut current) = current_client.take() {
                current_client = if current.get().is_empty() {
                    current.remove();
                    let mut e = match blocks.last_entry() {
                        Some(e) => e,
                        None => break,
                    };
                    stack_head = e.get_mut().pop_front();
                    Some(e)
                } else {
                    stack_head = current.get_mut().pop_front();
                    Some(current)
                }
            }
        }
        Ok(remaining)
    }

    fn apply_delete(&mut self, delete_set: &IDSet) -> crate::Result<IDSet> {
        let mut unapplied = IDSet::default();
        if delete_set.is_empty() {
            return Ok(unapplied);
        }
        // We can ignore the case of GC and Delete structs, because we are going to skip them
        for (&client, ranges) in delete_set.iter() {
            let current_clock = self.state.current_state.get(&client);

            for range in ranges.iter() {
                let clock_start = range.start;
                let clock_end = range.end;
                if clock_start < current_clock {
                    // range exists within already integrated blocks
                    if current_clock < clock_end {
                        unapplied.insert(ID::new(client, clock_start), clock_end - current_clock);
                    }

                    // We can ignore the case of GC and Delete structs, because we are going to skip them
                    if let Some(mut block) = self
                        .cursor
                        .seek_containing(ID::new(client, clock_start))
                        .optional()?
                    {
                        if block.id().client != client {
                            continue; // we shoot over the current client range
                        }

                        if !block.is_deleted() && block.id().clock < clock_start {
                            // split the first item if necessary
                            let offset = clock_start - block.id().clock;
                            // block is the same as right, but we need specifically its reference residing in the db
                            self.cursor.split_current(offset)?;
                            block = self.cursor.current()?;
                        }

                        while block.id().client == client && block.id().clock < clock_end {
                            if !block.is_deleted() {
                                if block.id().clock + block.clock_len() > clock_end {
                                    let offset = clock_end - block.id().clock;
                                    self.cursor.split_current(offset)?;
                                    block = self.cursor.prev()?.unwrap();
                                }
                                let mut block: BlockMut = block.into();
                                block.set_deleted();
                                self.cursor.update_current(*block.id(), block.header())?;
                                self.state.delete_set.insert(*block.id(), block.clock_len());
                            }
                            block = match self.cursor.next()? {
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
            if let Some(origin) = &insert.block.origin_left()
                && origin.client != insert.id().client
                && origin.clock >= local_sv.get(&origin.client)
            {
                return Some(origin.client);
            }

            if let Some(right_origin) = &insert.block.origin_right()
                && right_origin.client != insert.id().client
                && right_origin.clock >= local_sv.get(&right_origin.client)
            {
                return Some(right_origin.client);
            }

            if let Some(Node::Nested(parent)) = insert.parent()
                && parent.client != insert.id().client
                && parent.clock >= local_sv.get(&parent.client)
            {
                return Some(parent.client);
            }
        }

        None
    }
}

impl<'tx> Deref for TxMutScope<'tx> {
    type Target = TxScope<'tx>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'tx> DerefMut for TxMutScope<'tx> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
