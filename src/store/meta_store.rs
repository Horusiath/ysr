use crate::store::{KEY_PREFIX_META, ReadableBytes};
use lmdb_rs_m::{Database, MdbError};
use smallvec::SmallVec;
use std::fmt::{Debug, Formatter};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct MetaStore<'tx> {
    db: &'tx Database<'tx>,
}

impl<'tx> MetaStore<'tx> {
    pub fn new(db: &'tx Database<'tx>) -> Self {
        Self { db }
    }

    pub fn get(&mut self, key: &str) -> crate::Result<Option<&'tx [u8]>> {
        let key = meta_key(key);
        match self.db.get(&key.as_ref()) {
            Ok(value) => Ok(Some(value)),
            Err(MdbError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn insert(&mut self, key: &str, value: &[u8]) -> crate::Result<()> {
        let key = meta_key(key);
        self.db.set(&key.as_ref(), &value)?;
        Ok(())
    }

    pub fn iter(&self) -> Iter<'_> {
        Iter::UnInit(self.db)
    }

    pub fn inspect(&self) -> Inspector<'_> {
        Inspector { store: *self }
    }
}

fn meta_key(key: &str) -> SmallVec<[u8; 24]> {
    let mut buf = SmallVec::with_capacity(1 + key.len());
    buf.push(KEY_PREFIX_META);
    buf.extend_from_slice(key.as_bytes());
    buf
}

pub enum Iter<'a> {
    UnInit(&'a Database<'a>),
    Init(lmdb_rs_m::Cursor<'a>),
}

impl<'a> Iter<'a> {
    pub fn next(&mut self) -> crate::Result<Option<(&'a str, &'a [u8])>> {
        match self {
            Iter::UnInit(db) => {
                let mut cursor = db.new_cursor()?;
                match cursor.to_gte_key(&[KEY_PREFIX_META].as_ref()) {
                    Ok(_) => {
                        let key: &'a [u8] = cursor.get_key()?;
                        let key: &'a str = unsafe { std::str::from_utf8_unchecked(key) };
                        let value: &'a [u8] = cursor.get_value()?;
                        *self = Iter::Init(cursor);
                        Ok(Some((key, value)))
                    }
                    Err(MdbError::NotFound) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Iter::Init(cursor) => match cursor.to_next_key() {
                Ok(_) => {
                    let key: &'a [u8] = cursor.get_key()?;
                    if key[0] != KEY_PREFIX_META {
                        return Ok(None);
                    }
                    let key: &'a str = unsafe { std::str::from_utf8_unchecked(key) };
                    let value: &'a [u8] = cursor.get_value()?;
                    Ok(Some((key, value)))
                }
                Err(MdbError::NotFound) => Ok(None),
                Err(e) => Err(e.into()),
            },
        }
    }
}

pub struct Inspector<'a> {
    store: MetaStore<'a>,
}

impl<'a> Debug for Inspector<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_map();
        let mut iter = self.store.iter();
        while let Some((k, v)) = iter.next().map_err(|_| std::fmt::Error)? {
            let bytes = ReadableBytes::new(v);
            s.key(&k);
            s.value(&bytes);
        }
        s.finish()
    }
}
