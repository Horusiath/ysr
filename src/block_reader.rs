use crate::block::{
    BlockHeader, BlockMut, CONTENT_TYPE_ATOM, CONTENT_TYPE_BINARY, CONTENT_TYPE_DELETED,
    CONTENT_TYPE_DOC, CONTENT_TYPE_EMBED, CONTENT_TYPE_FORMAT, CONTENT_TYPE_GC, CONTENT_TYPE_JSON,
    CONTENT_TYPE_MOVE, CONTENT_TYPE_NODE, CONTENT_TYPE_SKIP, CONTENT_TYPE_STRING, ID,
};
use crate::content::ContentType;
use crate::node::{NodeHeader, NodeID, NodeType};
use crate::read::{Decoder, ReadExt};
use crate::write::WriteExt;
use crate::{ClientID, Clock};
use bytes::BytesMut;
use smallvec::SmallVec;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use zerocopy::IntoBytes;

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
            current_client: ClientID::new(0),
            current_clock: Clock::new(0),
        })
    }

    fn next_block(&mut self) -> crate::Result<Option<Carrier>> {
        while self.remaining_blocks == 0 && self.remaining_clients > 0 {
            self.remaining_blocks = self.decoder.read_var()?;
            self.current_client = self.decoder.read_client()?;
            self.current_clock = self.decoder.read_var()?;
            self.remaining_clients -= 1;
        }

        if self.remaining_clients == 0 {
            return Ok(None);
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
                let block = self.read_block(block_id, info)?;
                self.remaining_blocks -= 1;
                self.current_clock += block.clock_len();
                Ok(Some(Carrier::Block(block)))
            }
        }
    }

    fn read_block(&mut self, id: ID, info: u8) -> crate::Result<BlockMut> {
        let mut block = BlockMut::new(id, BytesMut::zeroed(BlockHeader::SIZE))?;
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
                let mut root_parent_name = SmallVec::<[u8; 16]>::new();
                self.decoder.read_string(&mut root_parent_name)?;
                NodeID::from_root(&root_parent_name)
            } else {
                let nested_parent_id = self.decoder.read_left_id()?;
                NodeID::from_nested(nested_parent_id)
            };
            block.set_parent(parent_id);
        }
        let entry_key = if cannot_copy_parent_info && (info & HAS_PARENT_SUB) != 0 {
            let mut entry_key = SmallVec::<[u8; 16]>::new();
            self.decoder.read_string(&mut entry_key)?;
            entry_key
        } else {
            SmallVec::default()
        };
        self.init_content(info, &mut block)?;
        if !entry_key.is_empty() {
            block.init_entry_key(&entry_key)?;
        }
        Ok(block)
    }

    fn init_content(&mut self, info: u8, block: &mut BlockMut) -> crate::Result<()> {
        let content_type = info & CARRIER_INFO;
        block.set_content_type(content_type);
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

fn copy_content<D: Decoder>(decoder: &mut D, block: &mut BlockMut) -> crate::Result<()> {
    let count = decoder.read_len()?;
    block.set_clock_len(count);
    println!("({})", count);
    let mut buf = Vec::new();
    let mut writer = block.as_writer();
    for _ in 0u64..count.into() {
        decoder.read_bytes(&mut buf)?;
        writer.write_bytes(&buf)?;
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
    Block(BlockMut),
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

pub struct BlockRange {
    head: ID,
    len: Clock,
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
    use crate::block_reader::BlockReader;
    use crate::read::DecoderV1;
    use std::io::Cursor;

    #[test]
    fn decode_basic_v1() {
        let update = &[
            1, 3, 227, 214, 245, 198, 5, 0, 4, 1, 4, 116, 121, 112, 101, 1, 48, 68, 227, 214, 245,
            198, 5, 0, 1, 49, 68, 227, 214, 245, 198, 5, 1, 1, 50, 0,
        ];
        let mut decoder = DecoderV1::new(Cursor::new(update));
        let reader = BlockReader::new(&mut decoder).unwrap();
        for res in reader {
            let carrier = res.unwrap();
            println!("{}", carrier);
        }
    }
}
