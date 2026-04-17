use crate::content::{ContentType, FormatAttribute};
use crate::lmdb::Database;
use crate::node::{Node, NodeType};
use crate::store::Db;
use crate::store::block_store::BlockStore;
use crate::store::content_store::ContentStore;
use crate::store::map_entries::MapEntries;
use crate::{Block, Clock, Error, ID, Out, Unmounted, lib0};
use serde::de::{
    DeserializeOwned, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor,
};
use serde::{Deserialize, Deserializer};
use serde_json::de::SliceRead;
use std::fmt::Display;
use std::io::Cursor;

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Error::Custom(msg.to_string().into())
    }
}

pub trait Materialize: Sized {
    fn materialize<'tx, 'db>(block: Block<'tx>, db: &'tx Database<'db>) -> crate::Result<Self>;
    fn materialize_fragment<'tx, 'db>(
        block: Block<'tx>,
        db: &'tx Database<'db>,
        offset: usize,
    ) -> crate::Result<Self>;
}

impl Materialize for Out {
    fn materialize<'tx, 'db>(block: Block<'tx>, db: &'tx Database<'db>) -> crate::Result<Self> {
        if block.is_deleted() {
            Err(Error::NotFound)
        } else if block.content_type() == ContentType::Node {
            let node_id = *block.id();
            Ok(Out::Node(node_id))
        } else {
            let deserializer = BlockDeserializer::new(block, db.blocks(), db.contents());
            Ok(Out::Value(lib0::Value::deserialize(deserializer)?))
        }
    }

    fn materialize_fragment<'tx, 'db>(
        block: Block<'tx>,
        db: &'tx Database<'db>,
        offset: usize,
    ) -> crate::Result<Self> {
        if block.content_type() == ContentType::Node {
            let node_id = *block.id();
            Ok(Out::Node(node_id))
        } else {
            let value = lib0::Value::materialize_fragment(block, db, offset)?;
            Ok(Out::Value(value))
        }
    }
}

impl<T: DeserializeOwned> Materialize for T {
    /// Materialize entire block, possibly with all subsequent elements.
    fn materialize<'tx, 'db>(block: Block<'tx>, db: &'tx Database<'db>) -> crate::Result<Self> {
        if block.is_deleted() {
            return Err(Error::NotFound);
        }
        let deserializer = BlockDeserializer::new(block, db.blocks(), db.contents());
        T::deserialize(deserializer)
    }

    fn materialize_fragment<'tx, 'db>(
        block: Block<'tx>,
        db: &'tx Database<'db>,
        offset: usize,
    ) -> crate::Result<Self> {
        if block.clock_len() == Clock::new(1) {
            Self::materialize(block, db)
        } else {
            let mut id = *block.id();
            id.clock += Clock::new(offset as u32);
            let data = db.contents().get(id)?;
            match block.content_type() {
                ContentType::Json => Ok(serde_json::from_slice(data)?),
                ContentType::Atom => Ok(lib0::from_slice(data)?),
                content_type => Err(Error::UnsupportedContent(content_type as u8)),
            }
        }
    }
}

impl<Cap> Materialize for Unmounted<Cap> {
    fn materialize<'tx, 'db>(block: Block<'tx>, _: &'tx Database<'db>) -> crate::Result<Self> {
        if block.is_deleted() {
            Err(Error::NotFound)
        } else if block.content_type() != ContentType::Node {
            Err(Error::InvalidMapping("node"))
        } else {
            let node_id = *block.id();
            Ok(Unmounted::new(Node::from(node_id)))
        }
    }

    fn materialize_fragment<'tx, 'db>(
        block: Block<'tx>,
        db: &'tx Database<'db>,
        offset: usize,
    ) -> crate::Result<Self> {
        if offset == 0 {
            // only node types are supported, and node blocks are always fragmented
            Self::materialize(block, db)
        } else {
            Err(Error::OutOfRange)
        }
    }
}

pub(crate) struct BlockDeserializer<'de> {
    block: Block<'de>,
    blocks: BlockStore<'de>,
    content_store: ContentStore<'de>,
}

impl<'de> BlockDeserializer<'de> {
    pub fn new(
        block: Block<'de>,
        blocks: BlockStore<'de>,
        content_store: ContentStore<'de>,
    ) -> Self {
        BlockDeserializer {
            block,
            blocks,
            content_store,
        }
    }
}

impl<'de> Deserializer<'de> for BlockDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let content_type = self.block.content_type();
        match content_type {
            ContentType::Deleted => visitor.visit_unit(),
            ContentType::Json | ContentType::Atom => {
                let bytes = read_block_data(&self.block, &self.content_store)?;
                if content_type == ContentType::Atom {
                    let mut deserializer = lib0::de::Deserializer::new(Cursor::new(bytes));
                    Ok(deserializer.deserialize_any(visitor)?)
                } else {
                    let mut deserializer = serde_json::de::Deserializer::new(SliceRead::new(bytes));
                    Ok(deserializer.deserialize_any(visitor)?)
                }
            }
            ContentType::Binary => {
                let bytes = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_bytes(bytes)
            }
            ContentType::String => {
                let bytes = read_block_data(&self.block, &self.content_store)?;
                let str = unsafe { std::str::from_utf8_unchecked(bytes) };
                visitor.visit_str(str)
            }
            ContentType::Format => {
                let bytes = read_block_data(&self.block, &self.content_store)?;
                let fmt_attr = FormatAttribute::new(bytes)
                    .ok_or_else(|| Error::InvalidMapping("format attribute"))?;
                visitor.visit_map(FormatAttributeDeserializer::new(fmt_attr))
            }
            ContentType::Embed => unreachable!(),
            ContentType::Doc => visitor.visit_unit(),
            ContentType::Node => NodeDeserializer::from(self).deserialize_any(visitor),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_bool(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_bool(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("bool")),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_i8(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_i8(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("i8")),
        }
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_i16(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_i16(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("i16")),
        }
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_i32(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_i32(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("i32")),
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_i64(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_i64(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("i64")),
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_u8(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_u8(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("u8")),
        }
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_u16(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_u16(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("u16")),
        }
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_u32(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_u32(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("u32")),
        }
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_u64(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_u64(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("u64")),
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_f32(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_f32(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("f32")),
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_f64(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_f64(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("f64")),
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_char(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = self.block.try_inline_data().unwrap();
                visitor.visit_char(lib0::from_slice(data)?)
            }
            _ => Err(Error::InvalidMapping("char")),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_str(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = read_block_data(&self.block, &self.content_store)?;
                let str: String = lib0::from_slice(data)?;
                visitor.visit_string(str)
            }
            ContentType::String => {
                let data = read_block_data(&self.block, &self.content_store)?;
                let str = unsafe { std::str::from_utf8_unchecked(data) };
                visitor.visit_str(str)
            }
            _ => Err(Error::InvalidMapping("string")),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_byte_buf(serde_json::from_slice(data)?)
            }
            ContentType::Atom => {
                let data = read_block_data(&self.block, &self.content_store)?;
                let bytes: Vec<u8> = lib0::from_slice(data)?;
                visitor.visit_byte_buf(bytes)
            }
            ContentType::Binary => {
                let data = read_block_data(&self.block, &self.content_store)?;
                visitor.visit_bytes(data)
            }
            _ => Err(Error::InvalidMapping("bytes")),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.block.is_deleted() {
            visitor.visit_none()
        } else {
            match self.block.content_type() {
                ContentType::Deleted => visitor.visit_none(),
                ContentType::Json => {
                    let data = read_block_data(&self.block, &self.content_store)?;
                    let value: serde_json::Value = serde_json::from_slice(data)?;
                    Ok(value.deserialize_option(visitor)?)
                }
                ContentType::Atom => {
                    let data = read_block_data(&self.block, &self.content_store)?;
                    let value: lib0::Value = serde_json::from_slice(data)?;
                    Ok(value.deserialize_option(visitor)?)
                }
                _ => Err(Error::InvalidMapping("option")),
            }
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.block.is_deleted() {
            visitor.visit_unit()
        } else {
            match self.block.content_type() {
                ContentType::Deleted | ContentType::Doc => visitor.visit_unit(),
                ContentType::Json => {
                    let data = read_block_data(&self.block, &self.content_store)?;
                    let value: serde_json::Value = serde_json::from_slice(data)?;
                    Ok(value.deserialize_unit(visitor)?)
                }
                ContentType::Atom => {
                    let data = read_block_data(&self.block, &self.content_store)?;
                    let value: lib0::Value = serde_json::from_slice(data)?;
                    Ok(value.deserialize_unit(visitor)?)
                }
                _ => Err(Error::InvalidMapping("unit")),
            }
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.content_type() {
            ContentType::Json => {
                let data = read_block_data(&self.block, &self.content_store)?;
                let value: serde_json::Value = serde_json::from_slice(data)?; //TODO: optimize
                Ok(value.deserialize_map(visitor)?)
            }
            ContentType::Atom => {
                let data = read_block_data(&self.block, &self.content_store)?;
                let value: lib0::Value = lib0::from_slice(data)?; //TODO: optimize
                Ok(value.deserialize_map(visitor)?)
            }
            ContentType::Format => {
                let data = read_block_data(&self.block, &self.content_store)?;
                let fmt_attr = FormatAttribute::new(data)
                    .ok_or_else(|| Error::InvalidMapping("format attribute"))?;
                visitor.visit_map(FormatAttributeDeserializer::new(fmt_attr))
            }
            ContentType::Node => visitor.visit_map(MapNodeDeserializer::from(self)),
            _ => Err(Error::InvalidMapping("map")),
        }
    }

    #[inline]
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(&hash) = self.block.key_hash() {
            let db: Database<'_> = self.blocks.into();
            let map_entries = db.map_entries();
            let mut keys = map_entries.keys_for_hash(*self.block.parent(), hash);
            match keys.next()? {
                Some((key, _id)) => visitor.visit_str(key),
                None => Err(Error::InvalidMapping("identifier")),
            }
        } else {
            Err(Error::InvalidMapping("identifier"))
        }
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

struct NodeDeserializer<'de> {
    block: Block<'de>,
    blocks: BlockStore<'de>,
    content_store: ContentStore<'de>,
}

impl<'de> NodeDeserializer<'de> {
    fn new(block: Block<'de>, blocks: BlockStore<'de>, content_store: ContentStore<'de>) -> Self {
        NodeDeserializer {
            block,
            blocks,
            content_store,
        }
    }
}

impl<'de> Deserializer<'de> for NodeDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.node_type().unwrap() {
            NodeType::Unknown => visitor.visit_unit(),
            NodeType::List => {
                let deserializer = ListNodeDeserializer::new(self.block, self.blocks);
                visitor.visit_seq(deserializer)
            }
            NodeType::Map => {
                let deserializer = MapNodeDeserializer::new(self.block, self.blocks);
                visitor.visit_map(deserializer)
            }
            NodeType::Text => {
                let deserializer = TextNodeDeserializer::new(self.block, self.blocks, false);
                deserializer.deserialize_string(visitor)
            }
            NodeType::XmlText => {
                let deserializer = TextNodeDeserializer::new(self.block, self.blocks, true);
                deserializer.deserialize_string(visitor)
            }
            NodeType::XmlFragment => todo!(),
            NodeType::XmlElement => todo!(),
        }
    }

    fn deserialize_bool<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_i32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_i64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let xml_format = match self.block.node_type().unwrap() {
            NodeType::XmlText => true,
            _ => false,
        };
        let deserializer = TextNodeDeserializer::new(self.block, self.blocks, xml_format);
        deserializer.deserialize_string(visitor)
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("node"))
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let deserializer = ListNodeDeserializer::new(self.block, self.blocks);
        visitor.visit_seq(deserializer)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let deserializer = MapNodeDeserializer::new(self.block, self.blocks);
        visitor.visit_map(deserializer)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.block.node_type().unwrap() {
            NodeType::Text | NodeType::XmlText => self.deserialize_string(visitor),
            _ => self.deserialize_map(visitor),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }
}

impl<'de> From<BlockDeserializer<'de>> for NodeDeserializer<'de> {
    fn from(value: BlockDeserializer<'de>) -> Self {
        Self::new(value.block, value.blocks, value.content_store)
    }
}

fn read_block_data<'a, 'b>(
    block: &'a Block<'b>,
    content_store: &'a ContentStore<'b>,
) -> crate::Result<&'b [u8]> {
    debug_assert!(block.content_type() != ContentType::Node);

    match block.try_inline_data() {
        Some(data) => Ok(data),
        None => Ok(content_store.get(*block.id())?),
    }
}

struct ListNodeDeserializer<'de> {
    node: Block<'de>,
    blocks: BlockStore<'de>,
    content_store: ContentStore<'de>,
    current: Option<ID>,
}

impl<'de> ListNodeDeserializer<'de> {
    fn new(node: Block<'de>, blocks: BlockStore<'de>) -> Self {
        let content_store = blocks.into().contents();
        let start = node.start().copied();
        ListNodeDeserializer {
            node,
            blocks,
            content_store,
            current: start,
        }
    }
}

impl<'de> SeqAccess<'de> for ListNodeDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.current.take() {
            None => Ok(None),
            Some(block_Id) => {
                let block = self.blocks.get(block_Id)?;
                if !block.is_deleted() {
                    let deserializer =
                        BlockDeserializer::new(block, self.blocks, self.content_store);
                    seed.deserialize(deserializer).map(Some)
                } else {
                    // skip over deleted block
                    self.next_element_seed(seed)
                }
            }
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.node.node_len())
    }
}

struct MapNodeDeserializer<'de> {
    blocks: BlockStore<'de>,
    content_store: ContentStore<'de>,
    map_entries: MapEntries<'de>,
    current: Option<Block<'de>>,
}

impl<'de> MapNodeDeserializer<'de> {
    fn new(block: Block<'de>, blocks: BlockStore<'de>) -> Self {
        let content_store = blocks.into().contents();
        let map_entries = blocks.into().map_entries();
        let map_entries = map_entries.entries(block.id());
        MapNodeDeserializer {
            blocks,
            content_store,
            map_entries,
            current: None,
        }
    }
}

impl<'de> MapAccess<'de> for MapNodeDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.map_entries.next()? {
            None => Ok(None),
            Some(key) => {
                let id = *self.map_entries.block_id()?;
                let current = self.blocks.get(id)?;
                if !current.is_deleted() {
                    self.current = Some(current);
                    let deserializer: serde::de::value::StrDeserializer<'_, Error> =
                        key.key().into_deserializer();
                    let value: K::Value = seed.deserialize(deserializer)?;
                    Ok(Some(value))
                } else {
                    // skip over deleted entry and move to the next one
                    self.next_key_seed(seed)
                }
            }
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.current.take() {
            Some(block) => {
                let deserializer = BlockDeserializer::new(block, self.blocks, self.content_store);
                seed.deserialize(deserializer)
            }
            None => unreachable!(),
        }
    }
}

impl<'de> From<BlockDeserializer<'de>> for MapNodeDeserializer<'de> {
    fn from(value: BlockDeserializer<'de>) -> Self {
        let all_entries = value.blocks.into().map_entries();
        let map_entries = all_entries.entries(value.block.id());
        MapNodeDeserializer {
            map_entries,
            content_store: value.content_store,
            blocks: value.blocks,
            current: None,
        }
    }
}

struct FormatAttributeDeserializer<'de> {
    fmt: Option<FormatAttribute<'de>>,
}

impl<'de> FormatAttributeDeserializer<'de> {
    fn new(fmt: FormatAttribute<'de>) -> Self {
        FormatAttributeDeserializer { fmt: Some(fmt) }
    }
}

impl<'de> MapAccess<'de> for FormatAttributeDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if let Some(fmt) = self.fmt.take() {
            seed.deserialize(fmt.key().into_deserializer()).map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        if let Some(fmt) = self.fmt.take() {
            let value: lib0::Value = fmt.value()?;
            match seed.deserialize(value) {
                Ok(value) => Ok(value),
                Err(e) => Err(e.into()),
            }
        } else {
            unreachable!()
        }
    }
}

struct TextNodeDeserializer<'de> {
    node: Block<'de>,
    blocks: BlockStore<'de>,
    content_store: ContentStore<'de>,
    xml_format: bool,
    //TODO: maybe we could implement Deltas as a deserialization of that
}

impl<'de> TextNodeDeserializer<'de> {
    fn new(node: Block<'de>, blocks: BlockStore<'de>, xml_format: bool) -> Self {
        let content_store = blocks.into().contents();
        TextNodeDeserializer {
            node,
            blocks,
            content_store,
            xml_format,
        }
    }

    fn read_string(&self) -> crate::Result<String> {
        let mut current = self.node.start().copied();
        let mut buf = String::with_capacity(self.node.node_len());
        while let Some(next) = current {
            let block = self.blocks.get(next)?;
            if !block.is_deleted() {
                match block.content_type() {
                    ContentType::String => {
                        let data = read_block_data(&block, &self.content_store)?;
                        let str = unsafe { str::from_utf8_unchecked(data) };
                        buf.push_str(str);
                    }
                    ContentType::Format if self.xml_format => {
                        let data = read_block_data(&block, &self.content_store)?;
                        Self::push_fmt(&mut buf, data)?;
                    }
                    _ => { /* ignore */ }
                }
            }
            current = block.right().copied();
        }
        Ok(buf)
    }

    fn push_fmt(buf: &mut String, data: &[u8]) -> crate::Result<()> {
        use std::fmt::Write;

        let format = FormatAttribute::new(data).ok_or(Error::InvalidMapping("format attribute"))?;
        let key = format.key();
        let value: lib0::Value = format.value()?;
        match value {
            lib0::Value::Null | lib0::Value::Undefined => write!(buf, "</{}>", key).unwrap(),
            lib0::Value::Object(map) => {
                write!(buf, "<{}", key).unwrap();
                for (name, value) in map {
                    write!(buf, " {}=\"{}\"", name, value).unwrap();
                }
                write!(buf, ">").unwrap();
            }
            _ => { /* ignore */ }
        }
        Ok(())
    }
}

impl<'de> Deserializer<'de> for TextNodeDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_bool<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_i32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_i64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let string = self.read_string()?;
        visitor.visit_string(string)
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.node.start() {
            None => visitor.visit_none(),
            Some(_) => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!("maybe this could be used to support delta?")
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::InvalidMapping("string"))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let string = self.read_string()?;
        visitor.visit_string(string)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }
}
