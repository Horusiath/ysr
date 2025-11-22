use super::store::{
    KEY_PREFIX_BLOCK, KEY_PREFIX_CONTENT, KEY_PREFIX_INTERN_STR, KEY_PREFIX_MAP, KEY_PREFIX_META,
    KEY_PREFIX_STATE_VECTOR,
};
use crate::block::{BlockHeader, ID};
use crate::content::BlockContentRef;
use crate::node::NodeID;
use crate::{lib0, ClientID, Clock, U32};
use lmdb_rs_m::MdbError;
use std::fmt::{Debug, Display};
use zerocopy::{FromBytes, TryFromBytes};

pub struct DocInspector<'a> {
    cursor: lmdb_rs_m::Cursor<'a>,
}

impl<'a> DocInspector<'a> {
    pub fn new(cursor: lmdb_rs_m::Cursor<'a>) -> DocInspector<'a> {
        DocInspector { cursor }
    }

    fn current_entry(&mut self) -> crate::Result<Entry<'a>> {
        let key = self.cursor.get_key()?;
        let value = self.cursor.get_value()?;
        Entry::parse(key, value)
    }

    fn move_next(&mut self) -> crate::Result<()> {
        match self.cursor.to_next_key() {
            Ok(_) => Ok(()),
            Err(MdbError::NotFound) => Err(crate::Error::NotFound),
            Err(err) => Err(err.into()),
        }
    }

    pub fn print_all(&mut self) -> crate::Result<()> {
        for res in self {
            let entry = res?;
            println!("{}", entry);
        }
        Ok(())
    }
}

impl<'a> Iterator for DocInspector<'a> {
    type Item = crate::Result<Entry<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.move_next() {
            Ok(_) => { /* do nothing */ }
            Err(crate::Error::NotFound) => return None,
            Err(err) => return Some(Err(err)),
        }
        Some(self.current_entry())
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum Entry<'a> {
    Meta(&'a [u8], &'a [u8]) = KEY_PREFIX_META,
    InternString(&'a U32, &'a str) = KEY_PREFIX_INTERN_STR,
    StateVector(&'a ClientID, &'a Clock) = KEY_PREFIX_STATE_VECTOR,
    Block(&'a ID, &'a BlockHeader) = KEY_PREFIX_BLOCK,
    MapEntry(&'a NodeID, &'a str, &'a ID) = KEY_PREFIX_MAP,
    Content(&'a ID, &'a [u8]) = KEY_PREFIX_CONTENT,
}

impl<'a> Entry<'a> {
    pub fn parse(key: &'a [u8], value: &'a [u8]) -> crate::Result<Self> {
        let tag = key[0];
        let key = &key[1..];
        match tag {
            KEY_PREFIX_META => Ok(Entry::Meta(key, value)),
            KEY_PREFIX_INTERN_STR => {
                let hash =
                    U32::ref_from_bytes(key).map_err(|_| crate::Error::InvalidMapping("U32"))?;
                let str = std::str::from_utf8(value)
                    .map_err(|_| crate::Error::InvalidMapping("InternString"))?;
                Ok(Entry::InternString(hash, str))
            }
            KEY_PREFIX_STATE_VECTOR => {
                let client = ClientID::parse(key)?;
                let clock = Clock::ref_from_bytes(value)
                    .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                Ok(Entry::StateVector(client, clock))
            }
            KEY_PREFIX_BLOCK => {
                let id = ID::parse(key)?;
                let header = BlockHeader::try_ref_from_bytes(value)
                    .map_err(|_| crate::Error::InvalidMapping("BlockHeader"))?;
                Ok(Entry::Block(id, header))
            }
            KEY_PREFIX_MAP => {
                let node_id = NodeID::ref_from_bytes(&key[..size_of::<NodeID>()])
                    .map_err(|_| crate::Error::InvalidMapping("NodeID"))?;
                let key = &key[size_of::<NodeID>() + size_of::<U32>()..];
                let key = std::str::from_utf8(key)
                    .map_err(|_| crate::Error::InvalidMapping("MapEntry"))?;
                let id = ID::parse(value)?;
                Ok(Entry::MapEntry(node_id, key, id))
            }
            KEY_PREFIX_CONTENT => {
                let id = ID::parse(key)?;
                Ok(Entry::Content(id, value))
            }
            other => unimplemented!("unknown store keyspace tag: {}", other),
        }
    }
}

impl<'a> Display for Entry<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Entry::Meta(key, value) => {
                write!(f, "meta: ")?;
                fmt_bytes(f, key)?;
                write!(f, " => ")?;
                fmt_bytes(f, value)?;
                Ok(())
            }
            Entry::InternString(alias, string) => {
                write!(f, "intern-str: {} => \"{}\"", alias.get(), string)
            }
            Entry::StateVector(&client, clock) => {
                if client == ClientID::MAX_VALUE {
                    write!(f, "state-vector: root => {}", clock.get())
                } else {
                    write!(f, "state-vector: {} => {}", client, clock.get())
                }
            }
            Entry::Block(id, header) => {
                write!(f, "block: {} => {}", id, header)
            }
            Entry::MapEntry(node, key, id) => {
                write!(f, "map-entry: {}:{} => {}", node, key, id)
            }
            Entry::Content(id, content) => {
                write!(f, "content: {} => ", id)?;
                let content = BlockContentRef::new(content).map_err(|_| std::fmt::Error)?;
                Display::fmt(&content, f)
            }
        }
    }
}

fn fmt_bytes(f: &mut std::fmt::Formatter<'_>, bytes: &[u8]) -> std::fmt::Result {
    write!(f, "b\"")?;
    for &b in bytes {
        // https://doc.rust-lang.org/reference/tokens.html#byte-escapes
        if b == b'\n' {
            write!(f, "\\n")?;
        } else if b == b'\r' {
            write!(f, "\\r")?;
        } else if b == b'\t' {
            write!(f, "\\t")?;
        } else if b == b'\\' || b == b'"' {
            write!(f, "\\{}", b as char)?;
        } else if b == b'\0' {
            write!(f, "\\0")?;
        // ASCII printable
        } else if (0x20..0x7f).contains(&b) {
            write!(f, "{}", b as char)?;
        } else {
            write!(f, "\\x{:02x}", b)?;
        }
    }
    write!(f, "\"")?;
    Ok(())
}
