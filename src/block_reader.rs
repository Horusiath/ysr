use crate::block::{
    BlockHeader, BlockMut, CONTENT_TYPE_GC, CONTENT_TYPE_SKIP, ID, InsertBlockData,
};
use crate::content::{Content, ContentType, FormatAttribute};
use crate::id_set::IDSet;
use crate::integrate::IntegrationContext;
use crate::lmdb::Database;
use crate::node::{Node, NodeID, NodeType};
use crate::read::{Decode, Decoder, ReadExt};
use crate::store::Db;
use crate::transaction::TransactionState;
use crate::write::Encoder;
use crate::{ClientID, Clock, U32};
use bytes::{BufMut, BytesMut};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::io::Read;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[derive(Default)]
pub struct Update {
    pub(crate) blocks: BTreeMap<ClientID, VecDeque<Carrier>>,
    pub(crate) delete_set: IDSet,
}

impl Update {
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        let mut decoder = crate::read::DecoderV1::from_slice(bytes);
        Self::decode_with(&mut decoder)
    }

    pub fn decode_with<D: Decoder>(decoder: &mut D) -> crate::Result<Self> {
        // read blocks
        let blocks = Self::decode_blocks(decoder)?;
        // read delete set
        let delete_set = IDSet::decode_with(decoder)?;
        Ok(Update { blocks, delete_set })
    }

    fn decode_blocks<D: Decoder>(
        decoder: &mut D,
    ) -> crate::Result<BTreeMap<ClientID, VecDeque<Carrier>>> {
        // read blocks
        let clients_len: u32 = decoder.read_var()?;
        let mut clients = BTreeMap::new();

        for _ in 0..clients_len {
            let blocks_len = decoder.read_var::<u32>()? as usize;

            let client = decoder.read_client()?;
            let mut clock: Clock = decoder.read_var()?;
            let blocks = clients.entry(client).or_insert_with(VecDeque::new);
            // Attempt to pre-allocate memory for the blocks. If the capacity overflows and
            // allocation fails, return an error.
            blocks.try_reserve(blocks_len)?;

            for _ in 0..blocks_len {
                let id = ID::new(client, clock);
                if let Some(block) = Self::decode_block(id, decoder)? {
                    // due to bug in the past it was possible for empty bugs to be generated
                    // even though they had no effect on the document store
                    clock += block.len();
                    blocks.push_back(block);
                }
            }
        }
        Ok(clients)
    }

    fn decode_block<D: Decoder>(id: ID, decoder: &mut D) -> crate::Result<Option<Carrier>> {
        let info = decoder.read_info()?;
        match info & CARRIER_INFO {
            CONTENT_TYPE_GC => {
                let len = decoder.read_len()?;
                let end = id.clock + len - 1;
                Ok(Some(Carrier::GC(BlockRange::new(id, end))))
            }
            CONTENT_TYPE_SKIP => {
                let len = decoder.read_len()?;
                let end = id.clock + len - 1;
                Ok(Some(Carrier::Skip(BlockRange::new(id, end))))
            }
            _ => Self::read_block(id, info, decoder),
        }
    }

    fn read_block<D: Decoder>(id: ID, info: u8, decoder: &mut D) -> crate::Result<Option<Carrier>> {
        let mut header = BlockHeader::empty();
        let mut parent = None;
        let mut entry = None;
        let cannot_copy_parent_info = info & (HAS_RIGHT_ID | HAS_LEFT_ID) == 0;
        if info & HAS_LEFT_ID != 0 {
            let id = decoder.read_left_id()?;
            header.set_origin_left(id);
        }
        if info & HAS_RIGHT_ID != 0 {
            let id = decoder.read_right_id()?;
            header.set_origin_right(id);
        }
        if cannot_copy_parent_info {
            let parent_node = if decoder.read_parent_info()? {
                let mut root_parent_name = String::new();
                let buf = unsafe { root_parent_name.as_mut_vec() };
                decoder.read_string(buf)?;
                Node::root_named(root_parent_name)
            } else {
                let nested_parent_id = decoder.read_left_id()?;
                Node::Nested(NodeID::from_nested(nested_parent_id))
            };
            header.set_parent(parent_node.id());
            parent = Some(parent_node);
        }
        if cannot_copy_parent_info && (info & HAS_PARENT_SUB) != 0 {
            let mut writer = BytesMut::new().writer();
            decoder.read_string(&mut writer)?;
            let entry_key = writer.into_inner().freeze();

            let key_hash = twox_hash::XxHash32::oneshot(0, &entry_key);
            header.set_key_hash(Some(U32::new(key_hash)));
            entry = Some(entry_key);
        }
        let content_type = ContentType::try_from(info & CARRIER_INFO)?;
        header.set_content_type(content_type);

        let content = Self::read_content(&mut header, decoder)?;
        let block = InsertBlockData {
            block: BlockMut::new(id, header),
            content,
            parent,
            entry,
        };
        Ok(Some(Carrier::Block(block)))
    }

    fn read_content(
        block: &mut BlockHeader,
        decoder: &mut impl Decoder,
    ) -> crate::Result<SmallVec<[Content<'static>; 1]>> {
        //TODO: a lot of the byte copying below could be just implemented via slices rather than Vec writes
        let mut result = SmallVec::new();
        match block.content_type() {
            ContentType::Deleted => {
                let deleted_len = decoder.read_len()?;
                block.set_clock_len(deleted_len);
            }
            ContentType::Json => {
                let len = copy_json(decoder, &mut result)?;
                block.set_clock_len(len);
            }
            ContentType::Atom => {
                let len = copy_lib0(decoder, &mut result)?;
                block.set_clock_len(len);
            }
            ContentType::Binary => {
                let mut w = Vec::new();

                block.set_clock_len(1.into());
                let len = decoder.read_len()?;
                std::io::copy(&mut decoder.take(len.into()), &mut w)?;

                result.push(Content::new(ContentType::Binary, Cow::Owned(w)));
            }
            ContentType::String => {
                let mut w = Vec::new();

                let byte_len = decoder.read_len()?;
                std::io::copy(&mut decoder.take(byte_len.into()), &mut w)?;
                let str = unsafe { std::str::from_utf8_unchecked(&w) };
                let utf16_len = str.encode_utf16().count() as u32;
                block.set_clock_len(Clock::new(utf16_len));

                result.push(Content::new(ContentType::String, Cow::Owned(w)));
            }
            ContentType::Embed => {
                let mut w = Vec::new();

                block.set_clock_len(1.into());
                let json = decoder.read_json::<serde_json::Value>()?;
                serde_json::to_writer(&mut w, &json)?;

                result.push(Content::new(ContentType::Embed, Cow::Owned(w)));
            }
            ContentType::Format => {
                let buf = FormatAttribute::decode(decoder)?;
                block.set_clock_len(1.into());

                result.push(Content::new(ContentType::Format, Cow::Owned(buf)));
            }
            ContentType::Node => {
                block.set_clock_len(1.into());
                let type_ref = decoder.read_type_ref()?;
                let node_type = NodeType::try_from(type_ref)?;
                block.set_node_type(node_type);
            }
            ContentType::Doc => {
                block.set_clock_len(1.into());
                return Err(crate::Error::UnsupportedContent(ContentType::Doc as u8));
            }
        }
        Ok(result)
    }

    /// Merge two updates into one, deduplicating overlapping carriers.
    pub fn merge_updates(mut a: Self, mut b: Self) -> Self {
        let blocks =
            Self::merge_blocks(std::mem::take(&mut a.blocks), std::mem::take(&mut b.blocks));

        // merge delete sets
        a.delete_set.merge(b.delete_set);
        a.delete_set.squash();

        Update {
            blocks,
            delete_set: a.delete_set,
        }
    }

    fn merge_blocks(
        a: BTreeMap<ClientID, VecDeque<Carrier>>,
        b: BTreeMap<ClientID, VecDeque<Carrier>>,
    ) -> BTreeMap<ClientID, VecDeque<Carrier>> {
        let mut blocks_a = a.into_iter().peekable();
        let mut blocks_b = b.into_iter().peekable();
        let mut blocks = BTreeMap::new();

        loop {
            match (blocks_a.peek(), blocks_b.peek()) {
                (Some((ca, _)), Some((cb, _))) => {
                    let ca = *ca;
                    let cb = *cb;
                    if ca < cb {
                        let (client, carriers) = blocks_a.next().unwrap();
                        blocks.insert(client, carriers);
                    } else if ca > cb {
                        let (client, carriers) = blocks_b.next().unwrap();
                        blocks.insert(client, carriers);
                    } else {
                        // same client in both updates — merge carrier lists
                        let (client, carriers_a) = blocks_a.next().unwrap();
                        let (_, carriers_b) = blocks_b.next().unwrap();
                        let merged = Self::merge_carriers(carriers_a, carriers_b);
                        if !merged.is_empty() {
                            blocks.insert(client, merged);
                        }
                    }
                }
                (Some(_), None) => {
                    let (client, carriers) = blocks_a.next().unwrap();
                    blocks.insert(client, carriers);
                }
                (None, Some(_)) => {
                    let (client, carriers) = blocks_b.next().unwrap();
                    blocks.insert(client, carriers);
                }
                (None, None) => break,
            }
        }

        blocks
    }

    /// Merge two sorted carrier sequences for the same client into one.
    /// Handles overlapping carriers by keeping the higher-priority variant
    /// and splitting/deduplicating as needed.
    fn merge_carriers(mut a: VecDeque<Carrier>, mut b: VecDeque<Carrier>) -> VecDeque<Carrier> {
        let mut result = VecDeque::with_capacity(a.len() + b.len());

        loop {
            let carrier = match (a.front(), b.front()) {
                (Some(ca), Some(cb)) => {
                    if ca.id().clock <= cb.id().clock {
                        a.pop_front().unwrap()
                    } else {
                        b.pop_front().unwrap()
                    }
                }
                (Some(_), None) => a.pop_front().unwrap(),
                (None, Some(_)) => b.pop_front().unwrap(),
                (None, None) => break,
            };
            Self::push_carrier(&mut result, carrier);
        }

        result
    }

    /// Push a carrier into a sorted, non-overlapping result sequence.
    /// Handles overlaps with existing entries by splitting/deduplicating
    /// based on carrier priority, and tries to merge adjacent carriers.
    fn push_carrier(result: &mut VecDeque<Carrier>, mut carrier: Carrier) {
        let mut suffix: Option<Carrier> = None;

        loop {
            let Some(last) = result.back() else {
                result.push_back(carrier);
                break;
            };

            let last_end = last.end();
            let curr_start = carrier.id().clock;

            if curr_start > last_end {
                // No overlap: try to merge with the back or just push
                if result.back().unwrap().can_merge(&carrier) {
                    result.back_mut().unwrap().merge(carrier);
                } else {
                    result.push_back(carrier);
                }
                break;
            }

            // Overlap: curr_start <= last_end
            let curr_end = carrier.end();
            let last_prio = Carrier::priority(last);
            let curr_prio = Carrier::priority(&carrier);

            if last_prio >= curr_prio {
                // Existing entry wins in the overlap region
                if curr_end <= last_end {
                    // Carrier fully contained — discard it
                    break;
                }
                // Carrier extends beyond last — split off the non-overlapping tail
                let offset = last_end + 1 - curr_start;
                if let Some(right) = carrier.split(offset) {
                    carrier = right;
                    if result.back().unwrap().can_merge(&carrier) {
                        result.back_mut().unwrap().merge(carrier);
                    } else {
                        result.push_back(carrier);
                    }
                }
                break;
            }

            // Carrier wins (higher priority) — pop old entry
            let mut old_last = result.pop_back().unwrap();
            let old_start = old_last.id().clock;
            let old_end = old_last.end();

            if curr_start > old_start {
                // Old entry has a non-overlapping prefix to preserve
                let prefix_len = curr_start - old_start;
                let remainder = old_last.split(prefix_len);
                // old_last is now the prefix
                result.push_back(old_last);

                // Check if old entry also has a suffix beyond carrier
                if old_end > curr_end {
                    if let Some(mut rem) = remainder {
                        let skip = curr_end + 1 - rem.id().clock;
                        suffix = rem.split(skip).or(suffix);
                    }
                }
                // Prefix is adjacent to carrier — try merge or push
                if result.back().unwrap().can_merge(&carrier) {
                    result.back_mut().unwrap().merge(carrier);
                } else {
                    result.push_back(carrier);
                }
                break;
            }

            // No prefix: carrier starts at or before old entry.
            // Save suffix if old entry extends beyond carrier.
            if old_end > curr_end && suffix.is_none() {
                let skip = curr_end + 1 - old_start;
                suffix = old_last.split(skip);
            }
            // Continue loop — carrier might overlap with the new back of result
        }

        if let Some(suffix) = suffix {
            if result.back().is_some_and(|last| last.can_merge(&suffix)) {
                result.back_mut().unwrap().merge(suffix);
            } else {
                result.push_back(suffix);
            }
        }
    }
}

pub(crate) struct BlockReader<'a, D> {
    decoder: &'a mut D,
    remaining_clients: usize,
    remaining_blocks: usize,
    current_client: ClientID,
    current_clock: Clock,
}

impl<'a, D: Decoder> BlockReader<'a, D> {
    pub fn new(decoder: &'a mut D) -> crate::Result<Self> {
        let num_of_state_updates: usize = decoder.read_var()?;
        Ok(Self {
            decoder,
            remaining_clients: num_of_state_updates,
            remaining_blocks: 0,
            current_client: 0.into(),
            current_clock: Clock::new(0),
        })
    }

    fn next_block(&mut self) -> crate::Result<Option<Carrier>> {
        if self.remaining_blocks == 0 && self.remaining_clients == 0 {
            return Ok(None);
        }

        while self.remaining_blocks == 0 && self.remaining_clients > 0 {
            self.remaining_blocks = self.decoder.read_var()?;
            self.current_client = self.decoder.read_client()?;
            self.current_clock = self.decoder.read_var()?;
            self.remaining_clients -= 1;
        }

        let info = self.decoder.read_info()?;
        match info & CARRIER_INFO {
            CONTENT_TYPE_GC => {
                let len = self.decoder.read_len()?;
                let id = ID::new(self.current_client, self.current_clock);
                let end = self.current_clock + len - 1;
                let carrier = Carrier::GC(BlockRange::new(id, end));
                self.current_clock += len;
                self.remaining_blocks -= 1;
                Ok(Some(carrier))
            }
            CONTENT_TYPE_SKIP => {
                let len = self.decoder.read_len()?;
                let id = ID::new(self.current_client, self.current_clock);
                let end = self.current_clock + len - 1;
                let carrier = Carrier::Skip(BlockRange::new(id, end));
                self.current_clock += len;
                self.remaining_blocks -= 1;
                Ok(Some(carrier))
            }
            _ => {
                let block_id = ID::new(self.current_client, self.current_clock);
                match Update::read_block(block_id, info, self.decoder)? {
                    None => Ok(None),
                    Some(carrier) => {
                        self.remaining_blocks -= 1;
                        self.current_clock += carrier.len();
                        Ok(Some(carrier))
                    }
                }
            }
        }
    }
}

impl<'a, D: Decoder> Iterator for BlockReader<'a, D> {
    type Item = crate::Result<Carrier>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_block() {
            Ok(None) => None,
            Ok(Some(carrier)) => Some(Ok(carrier)),
            Err(err) => Some(Err(err)),
        }
    }
}

fn copy_lib0<D: Decoder>(
    decoder: &mut D,
    acc: &mut SmallVec<[Content<'static>; 1]>,
) -> crate::Result<Clock> {
    let count = decoder.read_len()?;
    acc.try_reserve(count.get() as usize)?;
    for _ in 0u64..count.into() {
        let mut buf = Vec::new();
        crate::lib0::copy(decoder, &mut buf)?;
        acc.push(Content::new(ContentType::Atom, Cow::Owned(buf)));
    }
    Ok(count)
}

fn copy_json<D: Decoder>(
    decoder: &mut D,
    acc: &mut SmallVec<[Content<'static>; 1]>,
) -> crate::Result<Clock> {
    let count = decoder.read_len()?;
    acc.try_reserve(count.get() as usize)?;
    for _ in 0u64..count.into() {
        let mut buf = Vec::new();
        let value: serde_json::Value = serde_json::from_reader(&mut *decoder)?;
        serde_json::to_writer(&mut buf, &value)?;
        acc.push(Content::new(ContentType::Json, Cow::Owned(buf)));
    }
    Ok(count)
}

const CARRIER_INFO: u8 = 0b0001_1111;
const HAS_LEFT_ID: u8 = 0b1000_0000;
const HAS_RIGHT_ID: u8 = 0b0100_0000;
const HAS_PARENT_SUB: u8 = 0b0010_0000;

#[repr(u8)]
#[derive(Debug)]
pub enum Carrier {
    GC(BlockRange) = 0,
    Skip(BlockRange) = 10,
    Block(InsertBlockData),
}

impl Display for Carrier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Carrier::GC(range) => write!(f, "gc({})", range),
            Carrier::Skip(range) => write!(f, "skip({})", range),
            Carrier::Block(block) => write!(f, "{}", block),
        }
    }
}

impl Carrier {
    pub fn id(&self) -> &ID {
        match self {
            Carrier::GC(range) => range.head(),
            Carrier::Skip(range) => range.head(),
            Carrier::Block(block) => block.id(),
        }
    }

    pub fn end(&self) -> Clock {
        match self {
            Carrier::GC(range) => range.end(),
            Carrier::Skip(range) => range.end(),
            Carrier::Block(block) => block.id().clock + block.clock_len() - 1,
        }
    }

    pub fn len(&self) -> Clock {
        match self {
            Carrier::GC(range) => range.len(),
            Carrier::Skip(range) => range.len(),
            Carrier::Block(block) => block.clock_len(),
        }
    }

    pub fn is_skip(&self) -> bool {
        matches!(self, Carrier::Skip(_))
    }

    #[inline(always)]
    pub fn integrate(
        self,
        offset: Clock,
        state: &mut TransactionState,
        db: &mut Database,
    ) -> crate::Result<()> {
        match self {
            Carrier::GC(range) => {
                let id = range.head();
                state
                    .current_state
                    .set_max(id.client, id.clock + range.len());
            }
            Carrier::Block(mut block) => {
                let id = *block.id();
                let blocks = db.blocks();
                let mut context = IntegrationContext::create(&mut block, offset, &blocks)?;
                state
                    .current_state
                    .set_max(id.client, id.clock + block.clock_len());
                block.integrate(db, state, &mut context)?;
            }
            Carrier::Skip(_) => { /* ignore skip blocks */ }
        }
        Ok(())
    }

    /// Returns the priority of this carrier for merge deduplication.
    /// GC (2) > Block (1) > Skip (0).
    fn priority(&self) -> u8 {
        match self {
            Carrier::GC(_) => 2, // highest priority - block existed and was already deleted
            Carrier::Block(_) => 1,
            Carrier::Skip(_) => 0, // lowest priority - only when block was not found yet
        }
    }

    /// Split this carrier at `offset` (measured in clock units from the start).
    /// Self becomes the left part. Returns the right part, or `None` if split
    /// is not possible (e.g. offset is 0 or beyond the end).
    pub fn split(&mut self, offset: Clock) -> Option<Self> {
        match self {
            Carrier::GC(range) | Carrier::Skip(range) => {
                let head = *range.head();
                let end = range.end();
                let split_clock = head.clock + offset;
                if offset.get() == 0 || split_clock > end {
                    return None;
                }
                let right = BlockRange::new(ID::new(head.client, split_clock), end);
                *range = BlockRange::new(head, split_clock - 1);
                match self {
                    Carrier::GC(_) => Some(Carrier::GC(right)),
                    Carrier::Skip(_) => Some(Carrier::Skip(right)),
                    _ => unreachable!(),
                }
            }
            Carrier::Block(data) => data.split(offset).map(Carrier::Block),
        }
    }

    /// Check whether this carrier can be merged with the `other` carrier
    /// that follows it (i.e. `other` is the right neighbor).
    pub fn can_merge(&self, other: &Self) -> bool {
        match (self, other) {
            (Carrier::GC(a), Carrier::GC(b)) | (Carrier::Skip(a), Carrier::Skip(b)) => {
                a.head().client == b.head().client && a.end() + 1 == b.head().clock
            }
            (Carrier::Block(a), Carrier::Block(b)) => a.block.can_merge(&b.as_block()),
            _ => false,
        }
    }

    /// Merge `other` into this carrier. The caller must ensure
    /// `self.can_merge(&other)` returned `true` beforehand.
    pub fn merge(&mut self, other: Self) {
        match (self, other) {
            (Carrier::GC(a), Carrier::GC(b)) | (Carrier::Skip(a), Carrier::Skip(b)) => {
                *a = BlockRange::new(*a.head(), b.end());
            }
            (Carrier::Block(a), Carrier::Block(b)) => {
                a.merge(b);
            }
            _ => {}
        }
    }

    pub fn encode<E: Encoder>(&self, w: &mut E) -> crate::Result<()> {
        match self {
            Carrier::GC(range) => {
                w.write_info(0)?; // BLOCK_GC_REF_NUMBER
                w.write_len(range.len())?;
            }
            Carrier::Skip(range) => {
                w.write_info(10)?; // BLOCK_SKIP_REF_NUMBER
                w.write_len(range.len())?;
            }
            Carrier::Block(data) => data.encode(w)?,
        }
        Ok(())
    }
}

#[repr(C)]
#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    Copy,
    Clone,
    FromBytes,
    KnownLayout,
    Immutable,
    IntoBytes,
    Default,
    Ord,
    PartialOrd,
)]
pub struct BlockRange {
    head: ID,
    end: Clock,
}

impl BlockRange {
    pub fn new(head: ID, end: Clock) -> Self {
        Self { head, end }
    }

    pub fn head(&self) -> &ID {
        &self.head
    }

    pub fn end(&self) -> Clock {
        self.end
    }

    pub fn len(&self) -> Clock {
        self.end - self.head.clock + 1
    }

    #[inline]
    pub fn contains(&self, id: &ID) -> bool {
        self.head.client == id.client && self.head.clock >= id.clock && self.end <= id.clock
    }

    pub fn offset(&self, id: &ID) -> Option<Clock> {
        if self.contains(id) {
            Some(self.end - id.clock)
        } else {
            None
        }
    }

    #[inline]
    pub fn parse(data: &[u8]) -> crate::Result<&Self> {
        Self::ref_from_bytes(data).map_err(|_| crate::Error::InvalidMapping("BlockRange"))
    }
}

impl Display for BlockRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<{}:{}..{}>",
            self.head.client, self.head.clock, self.end
        )
    }
}

#[cfg(test)]
mod test {
    use crate::ClientID;
    use crate::block::ID;
    use crate::block_reader::{BlockReader, Carrier};
    use crate::read::DecoderV1;
    use std::io::Cursor;

    #[test]
    fn decode_basic_v1() {
        let update = &[
            1, 3, 227, 214, 245, 198, 5, 0, 4, 1, 4, 116, 121, 112, 101, 1, 48, 68, 227, 214, 245,
            198, 5, 0, 1, 49, 68, 227, 214, 245, 198, 5, 1, 1, 50, 0,
        ];
        let mut decoder = DecoderV1::new(Cursor::new(update));
        let mut reader = BlockReader::new(&mut decoder).unwrap();
        const CLIENT: ClientID = unsafe { ClientID::new_unchecked(1490905955) };
        // index: 0
        let Carrier::Block(n) = reader.next().unwrap().unwrap() else {
            unreachable!()
        };
        assert_eq!(n.id(), &ID::new(CLIENT, 0.into()));
        assert_eq!(n.block.origin_right(), None);
        assert_eq!(n.block.origin_left(), None);
        let Ok(text) = n.content[0].as_str() else {
            unreachable!()
        };
        assert_eq!(text, "0");

        // index: 1
        let Carrier::Block(n) = reader.next().unwrap().unwrap() else {
            unreachable!()
        };
        assert_eq!(n.id(), &ID::new(CLIENT, 1.into()));
        assert_eq!(n.block.origin_right(), Some(&ID::new(CLIENT, 0.into())));
        let Ok(text) = n.content[0].as_str() else {
            unreachable!()
        };
        assert_eq!(text, "1");

        // index: 2
        let Carrier::Block(n) = reader.next().unwrap().unwrap() else {
            unreachable!()
        };
        assert_eq!(n.id(), &ID::new(CLIENT, 2.into()));
        let Ok(text) = n.content[0].as_str() else {
            unreachable!()
        };
        assert_eq!(text, "2");

        // finish
        assert!(reader.next().is_none());
    }
}
