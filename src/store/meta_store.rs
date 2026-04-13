use crate::ClientID;
use crate::lmdb::{Cursor, Database, Error as LmdbError};
use crate::store::{KEY_PREFIX_META, ReadableBytes};
use smallvec::SmallVec;
use std::fmt::{Debug, Formatter};
use zerocopy::IntoBytes;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct MetaStore<'tx> {
    db: &'tx Database<'tx>,
}

impl<'tx> MetaStore<'tx> {
    pub const KEY_CLIENT_ID: &'static str = "$client_id";
    pub const KEY_PENDING: &'static str = "$pending";

    pub fn new(db: &'tx Database<'tx>) -> Self {
        Self { db }
    }

    /// Return a current store client ID or generate new one.
    pub fn client_id(&self) -> crate::Result<ClientID> {
        let data = self.get(Self::KEY_CLIENT_ID)?;
        match data {
            Some(id) => Ok(*ClientID::parse(id)?),
            None => {
                let client_id = ClientID::new_random();
                self.insert(Self::KEY_CLIENT_ID, client_id.as_bytes())?;
                Ok(client_id)
            }
        }
    }

    /// Get pending update if any exists.
    pub fn pending(&self) -> crate::Result<Option<&'tx [u8]>> {
        self.get(Self::KEY_PENDING)
    }

    /// Insert a new pending update, possibly replacing existing one.
    pub fn insert_pending(&self, update: &[u8]) -> crate::Result<()> {
        self.insert(Self::KEY_PENDING, update)
    }

    pub fn get(&self, key: &str) -> crate::Result<Option<&'tx [u8]>> {
        let key = meta_key(key);
        match self.db.get(key.as_ref()) {
            Ok(value) => Ok(Some(value)),
            Err(LmdbError::NOT_FOUND) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn insert(&self, key: &str, value: &[u8]) -> crate::Result<()> {
        let key = meta_key(key);
        self.db.put(key.as_ref(), value)?;
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
    Init(Cursor<'a>),
}

impl<'a> Iter<'a> {
    pub fn next(&mut self) -> crate::Result<Option<(&'a str, &'a [u8])>> {
        match self {
            Iter::UnInit(db) => {
                let mut cursor = db.cursor()?;
                match cursor.set_range(&[KEY_PREFIX_META]) {
                    Ok(_) => {
                        let key: &'a [u8] = cursor.key()?;
                        if key[0] != KEY_PREFIX_META {
                            return Ok(None);
                        }
                        let key: &'a str = unsafe { std::str::from_utf8_unchecked(&key[1..]) };
                        let value: &'a [u8] = cursor.value()?;
                        *self = Iter::Init(cursor);
                        Ok(Some((key, value)))
                    }
                    Err(LmdbError::NOT_FOUND) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Iter::Init(cursor) => match cursor.next() {
                Ok(_) => {
                    let key: &'a [u8] = cursor.key()?;
                    if key[0] != KEY_PREFIX_META {
                        return Ok(None);
                    }
                    let key: &'a str = unsafe { std::str::from_utf8_unchecked(&key[1..]) };
                    let value: &'a [u8] = cursor.value()?;
                    Ok(Some((key, value)))
                }
                Err(LmdbError::NOT_FOUND) => Ok(None),
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
