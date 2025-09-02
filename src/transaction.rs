use crate::block::{Block, BlockBuilder, BlockFlags, ID};
use crate::block_reader::{BlockRange, BlockReader, Carrier};
use crate::id_set::IDSet;
use crate::integrate::IntegrationContext;
use crate::node::{Node, NodeID, NodeType};
use crate::read::Decoder;
use crate::state_vector::Snapshot;
use crate::store::lmdb::store::SplitResult;
use crate::store::lmdb::BlockStore;
use crate::write::WriteExt;
use crate::{ClientID, StateVector};
use bitflags::bitflags;
use bytes::{Bytes, BytesMut};
use lmdb_rs_m::{Database, DbHandle};
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::io::Write;
use zerocopy::IntoBytes;

pub(crate) struct TransactionState {
    pub begin_state: StateVector,
    pub current_state: StateVector,
    pub origin: Option<Origin>,
    pub delete_set: IDSet,
}

impl TransactionState {
    fn new(begin_state: StateVector, origin: Option<Origin>) -> Self {
        let current_state = begin_state.clone();
        TransactionState {
            begin_state,
            current_state,
            origin,
            delete_set: IDSet::default(),
        }
    }

    fn precommit<'db>(
        &self,
        db: Database<'_>,
        summary: Option<&mut CommitSummary>,
    ) -> crate::Result<()> {
        todo!()
    }
}

pub struct Transaction<'db> {
    txn: lmdb_rs_m::Transaction<'db>,
    handle: DbHandle,
    state: Option<Box<TransactionState>>,
}

impl<'db> Transaction<'db> {
    pub(crate) fn read_write(
        txn: lmdb_rs_m::Transaction<'db>,
        handle: DbHandle,
        origin: Option<Origin>,
    ) -> Self {
        let state = origin.map(|o| {
            let db = txn.bind(&handle);
            let begin_state = db.state_vector().unwrap();
            Box::new(TransactionState::new(begin_state, Some(o)))
        });
        Self { txn, handle, state }
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
            Box::new(TransactionState::new(begin_state, None))
        });
        (db, state)
    }

    pub fn state_vector(&self) -> crate::Result<StateVector> {
        self.db().state_vector()
    }

    pub fn create_update(&self, since: &StateVector) -> crate::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.create_update_with(since, &mut buf)?;
        Ok(buf)
    }

    pub fn create_update_with<W: Write>(
        &self,
        since: &StateVector,
        writer: &mut W,
    ) -> crate::Result<()> {
        // wrote updates
        let current_state = self.state_vector()?;
        let diff = current_state.clear_present(since);
        writer.write_var(diff.len() as u64)?;
        let mut buf = BytesMut::new();
        for (&client_id, &clock) in diff.iter().rev() {
            let up_to = current_state.get(&client_id);
            let range = BlockRange::new(ID::new(client_id, clock), up_to - clock);
            /*let mut cursor = self.inner.block_range(range)?;
            buf.clear();
            let blocks_count = Self::write_updates(&mut cursor, &mut buf)?;
            writer.write_var(blocks_count)?;
            writer.write_var(client_id.get())?;
            writer.write_var(clock.get())?;
            writer.write_all(&buf)?;*/
        }

        // write delete set
        todo!()
    }

    fn write_updates(
        cursor: &mut impl Iterator<Item = crate::Result<crate::block::BlockBuilder>>,
        buf: &mut BytesMut,
    ) -> crate::Result<usize> {
        let mut blocks_count = 0;
        for block in cursor {
            let block = block?;
            blocks_count += 1;
            buf.extend_from_slice(block.as_bytes());
        }
        Ok(blocks_count)
    }

    pub fn apply_update<D: Decoder>(&mut self, decoder: &mut D) -> crate::Result<()> {
        let mut block_reader = BlockReader::new(decoder)?;
        let (mut db, state) = self.split_mut();
        let mut missing_sv = StateVector::default();
        //let mut skip_client = None;
        while let Some(res) = block_reader.next() {
            let carrier = res?;
            match carrier {
                Carrier::Block(mut block, parent_name) => {
                    let id = *block.id();
                    if state.current_state.contains(&id) {
                        let offset = state.current_state.get(&id.client) - id.clock;
                        if let Some(dep) = Self::missing(&block, &state.current_state) {
                            // current block is missing a dependency
                            /*

                            stack.push(block);
                            // get the struct reader that has the missing struct
                            match self.blocks.clients.get_mut(&dep) {
                                Some(block_refs) if !block_refs.is_empty() => {
                                    stack_head = block_refs.pop_front();
                                    current_target =
                                        self.blocks.clients.get_mut(&current_client_id);
                                    continue;
                                }
                                _ => {
                                    // This update message causally depends on another update message that doesn't exist yet
                                    missing_sv.set_min(dep, local_sv.get(&dep));
                                    Self::return_stack(stack, &mut self.blocks, &mut remaining);
                                    current_target =
                                        self.blocks.clients.get_mut(&current_client_id);
                                    stack = Vec::new();
                                }
                            }
                             */
                        } else if offset == 0 || offset < block.clock_len() {
                            let mut context = IntegrationContext::create(
                                &mut block,
                                parent_name.as_deref(),
                                offset,
                                &mut db,
                            )?;
                            state
                                .current_state
                                .set_max(id.client, id.clock + block.clock_len());
                            block.integrate(&mut db, state, &mut context)?;
                        }
                    } else {
                        // update from the same client is missing
                        let id = block.id();
                        missing_sv.set_min(id.client, id.clock - 1);
                        db.insert_block(block.as_ref())?;
                        // hid a dead wall, add all items from stack to restSS
                        /*
                        stack.push(block);
                        // hid a dead wall, add all items from stack to restSS
                        Self::return_stack(stack, &mut self.blocks, &mut remaining);
                        current_target = self.blocks.clients.get_mut(&current_client_id);
                        stack = Vec::new();
                         */
                    }
                }
                Carrier::GC(range) => {
                    if state.current_state.contains(range.head()) {
                        state
                            .current_state
                            .set_max(range.head().client, range.end());
                    } else {
                        todo!()
                    }
                }
                Carrier::Skip(range) => continue,
            }
            /*
                while let Some(mut block) = stack_head {
                    if !block.is_skip() {
                        let id = *block.id();
                        if local_sv.contains(&id) {
                            let offset = local_sv.get(&id.client) as i32 - id.clock as i32;
                            if let Some(dep) = Self::missing(&block, &local_sv) {
                                stack.push(block);
                                // get the struct reader that has the missing struct
                                match self.blocks.clients.get_mut(&dep) {
                                    Some(block_refs) if !block_refs.is_empty() => {
                                        stack_head = block_refs.pop_front();
                                        current_target =
                                            self.blocks.clients.get_mut(&current_client_id);
                                        continue;
                                    }
                                    _ => {
                                        // This update message causally depends on another update message that doesn't exist yet
                                        missing_sv.set_min(dep, local_sv.get(&dep));
                                        Self::return_stack(stack, &mut self.blocks, &mut remaining);
                                        current_target =
                                            self.blocks.clients.get_mut(&current_client_id);
                                        stack = Vec::new();
                                    }
                                }
                            } else if offset == 0 || (offset as u32) < block.len() {
                                let offset = offset as u32;
                                let client = id.client;
                                local_sv.set_max(client, id.clock + block.len());
                                if let BlockCarrier::Item(item) = &mut block {
                                    item.repair(store)?;
                                }
                                let should_delete = block.integrate(txn, offset);
                                let mut delete_ptr = if should_delete {
                                    let ptr = block.as_item_ptr();
                                    ptr
                                } else {
                                    None
                                };
                                store = txn.store_mut();
                                match block {
                                    BlockCarrier::Item(item) => {
                                        if item.parent != TypePtr::Unknown {
                                            store.blocks.push_block(item)
                                        } else {
                                            // parent is not defined. Integrate GC struct instead
                                            store.blocks.push_gc(BlockRange::new(item.id, item.len));
                                            delete_ptr = None;
                                        }
                                    }
                                    BlockCarrier::GC(gc) => store.blocks.push_gc(gc),
                                    BlockCarrier::Skip(_) => { /* do nothing */ }
                                }

                                if let Some(ptr) = delete_ptr {
                                    txn.delete(ptr);
                                }
                                store = txn.store_mut();
                            }
                        } else {
                            // update from the same client is missing
                            let id = block.id();
                            missing_sv.set_min(id.client, id.clock - 1);
                            stack.push(block);
                            // hid a dead wall, add all items from stack to restSS
                            Self::return_stack(stack, &mut self.blocks, &mut remaining);
                            current_target = self.blocks.clients.get_mut(&current_client_id);
                            stack = Vec::new();
                        }
                    }

                    // iterate to next stackHead
                    if !stack.is_empty() {
                        stack_head = stack.pop();
                    } else {
                        match current_target.take() {
                            Some(v) if !v.is_empty() => {
                                stack_head = v.pop_front();
                                current_target = Some(v);
                            }
                            _ => {
                                if let Some((client_id, target)) =
                                    Self::next_target(&mut client_block_ref_ids, &mut self.blocks)
                                {
                                    stack_head = target.pop_front();
                                    current_client_id = client_id;
                                    current_target = Some(target);
                                } else {
                                    // we're done
                                    break;
                                }
                            }
                        };
                    }
                }

                if remaining.is_empty() {
                    None
                } else {
                    Some(PendingUpdate {
                        update: Update {
                            blocks: remaining,
                            delete_set: DeleteSet::new(),
                        },
                        missing: missing_sv,
                    })
                }
            };

            let remaining_ds = txn.apply_delete(&self.delete_set).map(|ds| {
                let mut update = Update::new();
                update.delete_set = ds;
                update
            });

            Ok((remaining_blocks, remaining_ds))
                 */
        }
        Ok(())
    }

    fn missing(block: &BlockBuilder, local_sv: &StateVector) -> Option<ClientID> {
        if let Some(origin) = &block.origin_left() {
            if origin.client != block.id().client && origin.clock >= local_sv.get(&origin.client) {
                return Some(origin.client);
            }
        }

        if let Some(right_origin) = &block.origin_right() {
            if right_origin.client != block.id().client
                && right_origin.clock >= local_sv.get(&right_origin.client)
            {
                return Some(right_origin.client);
            }
        }

        if block.flags().contains(BlockFlags::HAS_PARENT) {
            let parent_id = block.parent();
            if parent_id.is_nested() {
                if parent_id.client != block.id().client
                    && parent_id.clock >= local_sv.get(&parent_id.client)
                {
                    return Some(parent_id.client);
                }
            }
        }

        None
        /*TODO: implement
        match &block.content() {
            ItemContent::Move(m) => {
                if let Some(start) = m.start.id() {
                    if start.clock >= local_sv.get(&start.client) {
                        return Some(start.client);
                    }
                }
                if !m.is_collapsed() {
                    if let Some(end) = m.end.id() {
                        if end.clock >= local_sv.get(&end.client) {
                            return Some(end.client);
                        }
                    }
                }
            }
            #[cfg(feature = "weak")]
            ItemContent::Type(branch) => {
                if let crate::types::TypeRef::WeakLink(source) = &branch.type_ref {
                    let start = source.quote_start.id();
                    let end = source.quote_end.id();
                    if let Some(start) = start {
                        if start.clock >= local_sv.get(&start.client) {
                            return Some(start.client);
                        }
                    }
                    if start != end {
                        if let Some(end) = &source.quote_end.id() {
                            if end.clock >= local_sv.get(&end.client) {
                                return Some(end.client);
                            }
                        }
                    }
                }
            }
            _ => { /* do nothing */ }
        }*/
    }

    pub fn commit(mut self, summary: Option<&mut CommitSummary>) -> crate::Result<()> {
        if let Some(state) = self.state.take() {
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
pub struct CommitSummary {
    flags: CommitFlags,
    update: BytesMut,
    changed_nodes: HashSet<NodeID>,
}

impl CommitSummary {
    pub fn new(flags: CommitFlags) -> Self {
        Self {
            flags,
            update: BytesMut::default(),
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
