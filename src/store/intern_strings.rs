use crate::Optional;
use crate::store::KEY_PREFIX_INTERN_STR;
use lmdb_rs_m::{Database, MdbError, MdbValue, ToMdbValue};
use std::fmt::{Debug, Formatter};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct InternStringsStore<'tx> {
    db: &'tx lmdb_rs_m::Database<'tx>,
}

impl<'tx> InternStringsStore<'tx> {
    pub const PREFIX: u8 = KEY_PREFIX_INTERN_STR;

    pub fn new(db: &'tx lmdb_rs_m::Database<'tx>) -> Self {
        Self { db }
    }

    pub fn intern(&mut self, value: &str) -> crate::Result<crate::U32> {
        let hash = twox_hash::XxHash32::oneshot(0, value.as_bytes());
        let hash = crate::U32::new(hash);
        self.insert(value, hash)?;
        Ok(hash)
    }

    pub fn insert(&mut self, value: &str, hash: crate::U32) -> crate::Result<()> {
        let key = InternStringsKey::new(hash);
        let mut cursor = self.db.new_cursor()?;
        match cursor.to_key(&key.as_bytes()) {
            Err(MdbError::NotFound) => {
                cursor.set(&key.as_bytes(), &value.as_bytes(), 0)?;
            }
            Ok(_) => {
                let existing: &str = cursor.get_value()?;
                if existing != value {
                    return Err(crate::Error::HashCollision(hash));
                }
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    pub fn get(&mut self, hash: crate::U32) -> crate::Result<Option<&'tx str>> {
        let key = InternStringsKey::new(hash);
        match self.db.get(&key) {
            Ok(value) => {
                let str = unsafe { std::str::from_utf8_unchecked(value) };
                Ok(Some(str))
            }
            Err(MdbError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn inspect(&self) -> Inspector<'tx> {
        Inspector { db: self.db }
    }

    pub fn iter(&mut self) -> Iter<'tx> {
        Iter::new(self.db)
    }
}

pub enum Iter<'tx> {
    UnInit(&'tx lmdb_rs_m::Database<'tx>),
    Init(lmdb_rs_m::Cursor<'tx>),
}

impl<'tx> Iter<'tx> {
    fn new(db: &'tx lmdb_rs_m::Database<'tx>) -> Self {
        Self::UnInit(db)
    }

    pub fn next(&mut self) -> crate::Result<Option<(&'tx crate::U32, &'tx str)>> {
        match self {
            Iter::UnInit(db) => {
                let mut cursor = db.new_cursor()?;
                match cursor.to_gte_key(&InternStringsKey::new(0.into())) {
                    Ok(_) => {
                        *self = Iter::Init(cursor);
                        self.current()
                    }
                    Err(MdbError::NotFound) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Iter::Init(cursor) => {
                cursor.to_next_key()?;
                self.current()
            }
        }
    }

    fn current(&mut self) -> crate::Result<Option<(&'tx crate::U32, &'tx str)>> {
        let cursor = if let Iter::Init(cursor) = self {
            cursor
        } else {
            unreachable!()
        };
        let key: &'tx [u8] = cursor.get_key()?;
        if key[0] != KEY_PREFIX_INTERN_STR {
            return Ok(None);
        }
        let hash = crate::U32::ref_from_bytes(&key[1..])
            .map_err(|_| crate::Error::InvalidMapping("intern string hash"))?;
        let string: &'tx str = cursor.get_value()?;
        Ok(Some((hash, string)))
    }
}

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Debug, PartialEq, Eq)]
pub struct InternStringsKey {
    tag: u8,
    hash: crate::U32,
}

impl InternStringsKey {
    pub fn new(hash: crate::U32) -> Self {
        InternStringsKey {
            tag: KEY_PREFIX_INTERN_STR,
            hash,
        }
    }

    pub fn parse(key: &[u8]) -> Option<&Self> {
        if let Ok(this) = Self::ref_from_bytes(key) {
            if this.tag == KEY_PREFIX_INTERN_STR {
                return Some(this);
            }
        }
        None
    }
}

impl ToMdbValue for InternStringsKey {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        let slice = self.as_bytes();
        unsafe { MdbValue::new(slice.as_ptr() as *const _, slice.len()) }
    }
}

pub struct Inspector<'tx> {
    db: &'tx Database<'tx>,
}

impl<'tx> Debug for Inspector<'tx> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_map();
        let mut cursor = self.db.new_cursor().map_err(|_| std::fmt::Error)?;
        cursor
            .to_gte_key(&InternStringsKey::new(0.into()))
            .map_err(|_| std::fmt::Error)?;
        let key: &[u8] = match cursor.get_key() {
            Ok(key) => key,
            Err(MdbError::NotFound) => return s.finish(),
            Err(_) => return Err(std::fmt::Error),
        };
        while let Some(id) = InternStringsKey::parse(key) {
            s.key(&id.hash);
            let value: &str = cursor.get_value().map_err(|_| std::fmt::Error)?;
            s.value(&value);

            cursor
                .to_next_key()
                .optional()
                .map_err(|_| std::fmt::Error)?;
        }
        s.finish()
    }
}
