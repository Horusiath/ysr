use crate::block::{
    BlockHeader, BlockMut, InsertBlockData, CONTENT_TYPE_GC, CONTENT_TYPE_SKIP, ID,
};
use crate::content::{BlockContent, ContentType};
use crate::id_set::IDSet;
use crate::integrate::IntegrationContext;
use crate::node::{Node, NodeID, NodeType};
use crate::read::{Decode, Decoder, ReadExt};
use crate::transaction::TransactionState;
use crate::write::WriteExt;
use crate::{ClientID, Clock, U32};
use bytes::{BufMut, BytesMut};
use lmdb_rs_m::Database;
use smallvec::SmallVec;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};

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

    fn decode_block<D: Decoder>(id: ID, decoder: &mut D) -> crate::Result<Option<Carrier>> {
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
                Node::root(root_parent_name)
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
        let mut block = InsertBlockData {
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
    ) -> crate::Result<SmallVec<[BlockContent; 1]>> {
        let mut result = SmallVec::new();
        match block.content_type() {
            ContentType::Deleted => {
                let deleted_len = decoder.read_len()?;
                block.set_clock_len(deleted_len);
            }
            ContentType::Json => block.set_clock_len(copy_json(decoder, &mut result)?),
            ContentType::Atom => block.set_clock_len(copy_lib0(decoder, &mut result)?),
            ContentType::Binary => {
                let mut w = BlockContent::new(ContentType::Binary);

                let len = decoder.read_len()?;
                block.set_clock_len(1.into());
                std::io::copy(&mut decoder.take(len.into()), &mut w)?;

                result.push(w)
            }
            ContentType::String => {
                let mut w = BlockContent::new(ContentType::String);

                let len = decoder.read_len()?;
                block.set_clock_len(len);
                std::io::copy(&mut decoder.take(len.into()), &mut w)?;

                result.push(w)
            }
            ContentType::Embed => {
                let mut w = BlockContent::new(ContentType::Embed);

                block.set_clock_len(1.into());
                let json = decoder.read_json::<serde_json::Value>()?;
                serde_json::to_writer(&mut w, &json)?;

                result.push(w)
            }
            ContentType::Format => {
                let mut w = BlockContent::new(ContentType::Format);

                block.set_clock_len(1.into());
                let key_len: u64 = decoder.read_var()?;
                w.write_var(key_len)?;
                std::io::copy(&mut decoder.take(key_len), &mut w)?;

                let value_len: u64 = decoder.read_var()?;
                w.write_var(value_len)?;
                std::io::copy(&mut decoder.take(value_len), &mut w)?;

                result.push(w)
            }
            ContentType::Node => {
                block.set_clock_len(1.into());
                let type_ref = decoder.read_type_ref()?;
                let node_type = NodeType::try_from(type_ref)?;
                block.set_node_type(node_type);
            }
            ContentType::Doc => {
                let mut w = BlockContent::new(ContentType::Doc);
                block.set_clock_len(1.into());
                todo!()
            }
        }
        Ok(result)
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
    res: &mut SmallVec<[BlockContent; 1]>,
) -> crate::Result<Clock> {
    let count = decoder.read_len()?;
    res.try_reserve(count.get() as usize)?;
    for _ in 0u64..count.into() {
        let mut content = BlockContent::new(ContentType::Atom);
        crate::lib0::copy(decoder, &mut content)?;
        res.push(content);
    }
    Ok(count)
}

fn copy_json<D: Decoder>(
    decoder: &mut D,
    res: &mut SmallVec<[BlockContent; 1]>,
) -> crate::Result<Clock> {
    let count = decoder.read_len()?;
    res.try_reserve(count.get() as usize)?;
    for _ in 0u64..count.into() {
        let mut content = BlockContent::new(ContentType::Atom);
        let value: serde_json::Value = serde_json::from_reader(&mut *decoder)?;
        serde_json::to_writer(&mut content, &value)?;
        res.push(content);
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
                let mut context = IntegrationContext::create(&mut block, offset, db)?;
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

#[derive(Debug)]
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
    use crate::content::BlockContentRef;
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
        let Carrier::Block(n) = reader.next().unwrap().unwrap() else {
            unreachable!()
        };
        assert_eq!(n.id(), &ID::new(CLIENT, 0.into()));
        assert_eq!(n.block.origin_right(), None);
        assert_eq!(n.block.origin_left(), None);
        let Some(text) = n.content.iter().next().unwrap().as_text() else {
            unreachable!()
        };
        assert_eq!(text, "0");

        // index: 1
        let Carrier::Block(n) = reader.next().unwrap().unwrap() else {
            unreachable!()
        };
        assert_eq!(n.id(), &ID::new(CLIENT, 1.into()));
        assert_eq!(n.block.origin_right(), Some(&ID::new(CLIENT, 0.into())));
        let Some(text) = n.content.iter().next().unwrap().as_text() else {
            unreachable!()
        };
        assert_eq!(text, "1");

        // index: 2
        let Carrier::Block(n) = reader.next().unwrap().unwrap() else {
            unreachable!()
        };
        assert_eq!(n.id(), &ID::new(CLIENT, 2.into()));
        let Some(text) = n.content.iter().next().unwrap().as_text() else {
            unreachable!()
        };
        assert_eq!(text, "2");

        // finish
        assert!(reader.next().is_none());
    }
}
