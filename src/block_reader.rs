use crate::block::{
    Block, BlockBuilder, BlockHeader, CONTENT_TYPE_ATOM, CONTENT_TYPE_BINARY, CONTENT_TYPE_DELETED,
    CONTENT_TYPE_DOC, CONTENT_TYPE_EMBED, CONTENT_TYPE_FORMAT, CONTENT_TYPE_GC, CONTENT_TYPE_JSON,
    CONTENT_TYPE_NODE, CONTENT_TYPE_SKIP, CONTENT_TYPE_STRING, ID,
};
use crate::id_set::IDSet;
use crate::integrate::IntegrationContext;
use crate::node::{NodeHeader, NodeID};
use crate::read::{Decode, Decoder, ReadExt};
use crate::transaction::TransactionState;
use crate::write::WriteExt;
use crate::{ClientID, Clock};
use bytes::BytesMut;
use lmdb_rs_m::Database;
use smallvec::SmallVec;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use zerocopy::IntoBytes;

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
            let blocks = clients.entry(client).or_insert_with(|| VecDeque::new());
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

    fn decode_block(id: ID, decoder: &mut impl Decoder) -> crate::Result<Option<Carrier>> {
        let info = decoder.read_info()?;
        match info & CARRIER_INFO {
            CONTENT_TYPE_GC => {
                let len = decoder.read_len()?;
                Ok(Some(Carrier::GC(BlockRange { head: id, len })))
            }
            CONTENT_TYPE_SKIP => {
                let len = decoder.read_len()?;
                Ok(Some(Carrier::Skip(BlockRange { head: id, len })))
            }
            _ => Self::read_block(id, info, decoder),
        }
    }

    fn read_block(id: ID, info: u8, decoder: &mut impl Decoder) -> crate::Result<Option<Carrier>> {
        let mut block = BlockBuilder::parse(id, BytesMut::zeroed(BlockHeader::SIZE))?;
        let mut parent_name = None;
        let cannot_copy_parent_info = info & (HAS_RIGHT_ID | HAS_LEFT_ID) == 0;
        if info & HAS_LEFT_ID != 0 {
            let left_id = decoder.read_left_id()?;
            block.set_origin_left(left_id);
        }
        if info & HAS_RIGHT_ID != 0 {
            let right_id = decoder.read_right_id()?;
            block.set_origin_right(right_id);
        }
        if cannot_copy_parent_info {
            let parent_id = if decoder.read_parent_info()? {
                let mut root_parent_name = String::new();
                let buf = unsafe { root_parent_name.as_mut_vec() };
                decoder.read_string(buf)?;
                let node = NodeID::from_root(&root_parent_name);
                parent_name = Some(root_parent_name);
                node
            } else {
                let nested_parent_id = decoder.read_left_id()?;
                NodeID::from_nested(nested_parent_id)
            };
            block.set_parent(parent_id);
        }
        if cannot_copy_parent_info && (info & HAS_PARENT_SUB) != 0 {
            let mut entry_key = SmallVec::<[u8; 16]>::new();
            decoder.read_string(&mut entry_key)?;
            block.init_entry_key(&entry_key)?;
        }
        Self::init_content(info, &mut block, decoder)?;
        Ok(Some(Carrier::Block(block, parent_name)))
    }

    fn init_content(
        info: u8,
        block: &mut BlockBuilder,
        decoder: &mut impl Decoder,
    ) -> crate::Result<()> {
        let content_type = info & CARRIER_INFO;
        block.set_content_type(content_type.try_into()?);
        match content_type {
            CONTENT_TYPE_GC => Err(crate::Error::UnsupportedContent(CONTENT_TYPE_GC)),
            CONTENT_TYPE_DELETED => {
                let deleted_len = decoder.read_len()?;
                block.set_clock_len(deleted_len);
                Ok(())
            }
            CONTENT_TYPE_JSON => copy_content(decoder, block),
            CONTENT_TYPE_ATOM => copy_content(decoder, block),
            CONTENT_TYPE_BINARY => {
                let len = decoder.read_len()?;
                block.set_clock_len(1.into());
                let mut w = block.as_writer();
                std::io::copy(&mut decoder.take(len.into()), &mut w)?;
                Ok(())
            }
            CONTENT_TYPE_STRING => {
                let len = decoder.read_len()?;
                block.set_clock_len(len);
                let mut w = block.as_writer();
                std::io::copy(&mut decoder.take(len.into()), &mut w)?;
                Ok(())
            }
            CONTENT_TYPE_EMBED => {
                block.set_clock_len(1.into());
                let json = decoder.read_json::<serde_json::Value>()?;
                serde_json::to_writer(block.as_writer(), &json)?;
                Ok(())
            }
            CONTENT_TYPE_FORMAT => {
                block.set_clock_len(1.into());
                let mut w = block.as_writer();
                let mut buf: SmallVec<[u8; 16]> = SmallVec::new();
                decoder.read_string(&mut buf)?;
                if buf.len() > u8::MAX as usize {
                    return Err(crate::Error::KeyTooLong);
                }
                w.write_u8(buf.len() as u8)?;
                w.write_all(&buf)?;
                let value: () = decoder.read_json()?;
                serde_json::to_writer(w, &value)?;
                Ok(())
            }
            CONTENT_TYPE_NODE => {
                block.set_clock_len(1.into());
                let type_ref = decoder.read_type_ref()?;
                let node_header = NodeHeader::new(type_ref);
                block.as_writer().write_all(node_header.as_bytes())?;
                Ok(())
            }
            CONTENT_TYPE_DOC => {
                block.set_clock_len(1.into());
                todo!()
            }
            CONTENT_TYPE_SKIP => Err(crate::Error::UnsupportedContent(CONTENT_TYPE_SKIP)),
            content_type => Err(crate::Error::UnsupportedContent(content_type)),
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
                let carrier = Carrier::GC(BlockRange {
                    head: ID::new(self.current_client, self.current_clock),
                    len,
                });
                self.current_clock += len;
                self.remaining_blocks -= 1;
                Ok(Some(carrier))
            }
            CONTENT_TYPE_SKIP => {
                let len = self.decoder.read_len()?;
                let carrier = Carrier::Skip(BlockRange {
                    head: ID::new(self.current_client, self.current_clock),
                    len,
                });
                self.current_clock += len;
                self.remaining_blocks -= 1;
                Ok(Some(carrier))
            }
            _ => {
                let block_id = ID::new(self.current_client, self.current_clock);
                let (block, parent_name) = self.read_block(block_id, info)?;
                self.remaining_blocks -= 1;
                self.current_clock += block.clock_len();
                Ok(Some(Carrier::Block(block, parent_name)))
            }
        }
    }

    fn read_block(&mut self, id: ID, info: u8) -> crate::Result<(BlockBuilder, Option<String>)> {
        let mut block = BlockBuilder::parse(id, BytesMut::zeroed(BlockHeader::SIZE))?;
        let mut parent_name = None;
        let cannot_copy_parent_info = info & (HAS_RIGHT_ID | HAS_LEFT_ID) == 0;
        if info & HAS_LEFT_ID != 0 {
            let left_id = self.decoder.read_left_id()?;
            block.set_origin_left(left_id);
        }
        if info & HAS_RIGHT_ID != 0 {
            let right_id = self.decoder.read_right_id()?;
            block.set_origin_right(right_id);
        }
        if cannot_copy_parent_info {
            let parent_id = if self.decoder.read_parent_info()? {
                let mut root_parent_name = String::new();
                let buf = unsafe { root_parent_name.as_mut_vec() };
                self.decoder.read_string(buf)?;
                let node = NodeID::from_root(&root_parent_name);
                parent_name = Some(root_parent_name);
                node
            } else {
                let nested_parent_id = self.decoder.read_left_id()?;
                NodeID::from_nested(nested_parent_id)
            };
            block.set_parent(parent_id);
        }
        if cannot_copy_parent_info && (info & HAS_PARENT_SUB) != 0 {
            let mut entry_key = SmallVec::<[u8; 16]>::new();
            self.decoder.read_string(&mut entry_key)?;
            block.init_entry_key(&entry_key)?;
        }
        self.init_content(info, &mut block)?;
        Ok((block, parent_name))
    }

    fn init_content(&mut self, info: u8, block: &mut BlockBuilder) -> crate::Result<()> {
        let content_type = info & CARRIER_INFO;
        block.set_content_type(content_type.try_into()?);
        match content_type {
            CONTENT_TYPE_GC => Err(crate::Error::UnsupportedContent(CONTENT_TYPE_GC)),
            CONTENT_TYPE_DELETED => {
                let deleted_len = self.decoder.read_len()?;
                block.set_clock_len(deleted_len);
                Ok(())
            }
            CONTENT_TYPE_JSON => copy_content(self.decoder, block),
            CONTENT_TYPE_ATOM => copy_content(self.decoder, block),
            CONTENT_TYPE_BINARY => {
                let len = self.decoder.read_len()?;
                block.set_clock_len(1.into());
                let mut w = block.as_writer();
                std::io::copy(&mut self.decoder.take(len.into()), &mut w)?;
                Ok(())
            }
            CONTENT_TYPE_STRING => {
                let len = self.decoder.read_len()?;
                block.set_clock_len(len);
                let mut w = block.as_writer();
                std::io::copy(&mut self.decoder.take(len.into()), &mut w)?;
                Ok(())
            }
            CONTENT_TYPE_EMBED => {
                block.set_clock_len(1.into());
                let json = self.decoder.read_json::<serde_json::Value>()?;
                serde_json::to_writer(block.as_writer(), &json)?;
                Ok(())
            }
            CONTENT_TYPE_FORMAT => {
                block.set_clock_len(1.into());
                let mut w = block.as_writer();
                let mut buf: SmallVec<[u8; 16]> = SmallVec::new();
                self.decoder.read_string(&mut buf)?;
                if buf.len() > u8::MAX as usize {
                    return Err(crate::Error::KeyTooLong);
                }
                w.write_u8(buf.len() as u8)?;
                w.write_all(&buf)?;
                let value = self.decoder.read_json()?;
                serde_json::to_writer(w, &value)?;
                Ok(())
            }
            CONTENT_TYPE_NODE => {
                block.set_clock_len(1.into());
                let type_ref = self.decoder.read_type_ref()?;
                let node_header = NodeHeader::new(type_ref);
                block.as_writer().write_all(node_header.as_bytes())?;
                Ok(())
            }
            CONTENT_TYPE_DOC => {
                block.set_clock_len(1.into());
                todo!()
            }
            CONTENT_TYPE_SKIP => Err(crate::Error::UnsupportedContent(CONTENT_TYPE_SKIP)),
            content_type => Err(crate::Error::UnsupportedContent(content_type)),
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

fn copy_content<D: Decoder>(decoder: &mut D, block: &mut BlockBuilder) -> crate::Result<()> {
    let count = decoder.read_len()?;
    block.set_clock_len(count);
    let mut buf = Vec::new();
    let mut writer = block.as_writer();
    for _ in 0u64..count.into() {
        decoder.read_bytes(&mut buf)?;
        writer.write_u32(buf.len() as u32)?;
        writer.write_all(&buf)?;
    }
    Ok(())
}

const CARRIER_INFO: u8 = 0b0001_1111;
const HAS_LEFT_ID: u8 = 0b1000_0000;
const HAS_RIGHT_ID: u8 = 0b0100_0000;
const HAS_PARENT_SUB: u8 = 0b0010_0000;

#[repr(u8)]
pub enum Carrier {
    GC(BlockRange) = 0,
    Skip(BlockRange) = 10,
    Block(BlockBuilder, Option<String>),
}

impl Display for Carrier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Carrier::GC(range) => write!(f, "gc({})", range),
            Carrier::Skip(range) => write!(f, "skip({})", range),
            Carrier::Block(block, parent_name) => write!(f, "{}", block),
        }
    }
}

impl Carrier {
    pub fn id(&self) -> &ID {
        match self {
            Carrier::GC(range) => range.head(),
            Carrier::Skip(range) => range.head(),
            Carrier::Block(block, _) => block.id(),
        }
    }

    pub fn end(&self) -> Clock {
        match self {
            Carrier::GC(range) => range.end(),
            Carrier::Skip(range) => range.end(),
            Carrier::Block(block, _) => block.id().clock + block.clock_len() - 1,
        }
    }

    pub fn len(&self) -> Clock {
        match self {
            Carrier::GC(range) => range.len(),
            Carrier::Skip(range) => range.len(),
            Carrier::Block(block, _) => block.clock_len(),
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
            Carrier::Block(mut block, parent_name) => {
                let id = *block.id();
                let mut context =
                    IntegrationContext::create(&mut block, parent_name.as_deref(), offset, db)?;
                state
                    .current_state
                    .set_max(id.client, id.clock + block.clock_len());
                block.integrate(db, state, &mut context)?;
            }
            Carrier::Skip(_) => { /* ignore skip blocks */ }
        }
        Ok(())
    }
}

pub struct BlockRange {
    head: ID,
    len: Clock,
}

impl BlockRange {
    pub fn new(head: ID, len: Clock) -> Self {
        Self { head, len }
    }

    pub fn head(&self) -> &ID {
        &self.head
    }

    pub fn end(&self) -> Clock {
        self.head.clock + self.len - 1
    }

    pub fn len(&self) -> Clock {
        self.len
    }
}

impl Display for BlockRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<{}:{}..{}>",
            self.head.client,
            self.head.clock,
            self.head.clock + self.len - 1
        )
    }
}

#[cfg(test)]
mod test {
    use crate::block::ID;
    use crate::block_reader::{BlockReader, Carrier};
    use crate::content::BlockContent;
    use crate::read::DecoderV1;
    use crate::ClientID;
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
        let Carrier::Block(n, _) = reader.next().unwrap().unwrap() else {
            unreachable!()
        };
        assert_eq!(n.id(), &ID::new(CLIENT, 0.into()));
        assert_eq!(n.origin_right(), None);
        assert_eq!(n.origin_left(), None);
        let BlockContent::Text(text) = n.content().unwrap() else {
            unreachable!()
        };
        assert_eq!(text, "0");

        // index: 1
        let Carrier::Block(n, _) = reader.next().unwrap().unwrap() else {
            unreachable!()
        };
        assert_eq!(n.id(), &ID::new(CLIENT, 1.into()));
        assert_eq!(n.origin_right(), Some(&ID::new(CLIENT, 0.into())));
        let BlockContent::Text(text) = n.content().unwrap() else {
            unreachable!()
        };
        assert_eq!(text, "1");

        // index: 2
        let Carrier::Block(n, _) = reader.next().unwrap().unwrap() else {
            unreachable!()
        };
        assert_eq!(n.id(), &ID::new(CLIENT, 2.into()));
        let BlockContent::Text(text) = n.content().unwrap() else {
            unreachable!()
        };
        assert_eq!(text, "2");

        // finish
        assert!(reader.next().is_none());
    }
}
