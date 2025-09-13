use crate::block::{Block, BlockBuilder, BlockFlags, ID};
use crate::block_reader::{BlockRange, BlockReader, Carrier, Update};
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
use std::collections::{BTreeMap, HashSet};
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
        summary: Option<&mut TransactionSummary>,
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

    pub fn incremental_update(&self) -> crate::Result<Vec<u8>> {
        todo!()
    }

    pub fn diff_update(&self, since: &StateVector) -> crate::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.diff_update_with(since, &mut buf)?;
        Ok(buf)
    }

    pub fn diff_update_with<W: Write>(
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
        let mut stack_head = block_reader.next();
        while let Some(res) = stack_head.take() {
            let carrier = res?;
            match carrier {
                Carrier::Block(mut block, parent_name) => {
                    let id = *block.id();
                    if state.current_state.contains(&id) {
                        // offset informs if current block partially overlaps with already integrated blocks
                        let offset = state.current_state.get(&id.client) - id.clock;
                        if let Some(dep) = Self::missing_dependency(&block, &state.current_state) {
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
                        stack_head =
                            Self::push_pending(block, &mut db, &mut block_reader, &mut missing_sv)?
                                .map(Ok);
                    }
                }
                Carrier::GC(range) => {
                    if state.current_state.contains(range.head()) {
                        // integrate GC by moving the current state clock
                        state
                            .current_state
                            .set_max(range.head().client, range.end());
                    } else {
                        // missing update prevents us from integrating the GC block,
                        // so just add it to the missing state vector
                        missing_sv.set_min(range.head().client, range.end());
                    }
                }
                Carrier::Skip(_) => continue,
            }
        }
        Ok(())
    }

    /// Push all pending blocks with the same client ID as `block` into the database.
    /// These blocks are not immediately integrated, since they are missing dependencies on other blocks.
    fn push_pending(
        block: BlockBuilder,
        db: &mut Database,
        reader: &mut BlockReader<impl Decoder>,
        missing_sv: &mut StateVector,
    ) -> crate::Result<Option<Carrier>> {
        let id = *block.id();
        missing_sv.set_min(id.client, id.clock - 1);
        db.insert_pending_block(block.as_ref())?;
        while let Some(res) = reader.next() {
            let carrier = res?;
            if carrier.id().client != id.client {
                // different client, return to caller
                return Ok(Some(carrier));
            }
            missing_sv.set_min(carrier.id().client, carrier.end());
            match carrier {
                Carrier::Block(block, name) => {
                    if let Some(name) = name {
                        let node = Node::root(name);
                        db.get_or_insert_node(node, NodeType::Unknown)?;
                    }
                    db.insert_pending_block(block.as_ref())?;
                }
                _ => { /* we don't insert GC or Skips */ }
            }
        }
        Ok(None)
    }

    /// Check if current `block` has any missing dependencies on other blocks that are not yet integrated.
    /// A dependency is missing if any of the block's origins (left, right, parent) point to a block that is not yet integrated.
    /// Returns the client ID of the missing dependency, or None if all dependencies are satisfied.
    fn missing_dependency(block: &BlockBuilder, local_sv: &StateVector) -> Option<ClientID> {
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
    }

    pub fn commit(mut self, summary: Option<&mut TransactionSummary>) -> crate::Result<()> {
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
