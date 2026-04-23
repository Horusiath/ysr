use crate::Optional;
use crate::lmdb::{Cursor, Database, Error as LmdbError};
use crate::store::KEY_PREFIX_INTERN_STR;
use std::fmt::{Debug, Formatter};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct InternStringsStore<'tx> {
    db: Database<'tx>,
}

impl<'tx> InternStringsStore<'tx> {
    pub const PREFIX: u8 = KEY_PREFIX_INTERN_STR;

    pub fn new(db: Database<'tx>) -> Self {
        Self { db }
    }

    pub fn intern(&self, value: &str) -> crate::Result<crate::U32> {
        let hash = twox_hash::XxHash32::oneshot(0, value.as_bytes());
        let hash = crate::U32::new(hash);
        self.insert(value, hash)?;
        Ok(hash)
    }

    pub fn insert(&self, value: &str, hash: crate::U32) -> crate::Result<()> {
        let key = InternStringsKey::new(hash);
        let mut cursor = self.db.cursor()?;
        match cursor.set_key(key.as_bytes()) {
            Err(LmdbError::NOT_FOUND) => {
                cursor.put(key.as_bytes(), value.as_bytes(), 0)?;
            }
            Ok((_, existing)) => {
                let existing = unsafe { std::str::from_utf8_unchecked(existing) };
                if existing != value {
                    return Err(crate::Error::HashCollision(hash));
                }
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    pub fn get(&self, hash: crate::U32) -> crate::Result<&'tx str> {
        let key = InternStringsKey::new(hash);
        match self.db.get(key.as_bytes()) {
            Ok(value) => {
                let str = unsafe { std::str::from_utf8_unchecked(value) };
                Ok(str)
            }
            Err(LmdbError::NOT_FOUND) => Err(crate::Error::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    pub fn inspect(&self) -> Inspector<'tx> {
        Inspector { db: self.db }
    }

    #[allow(unused)]
    pub fn iter(&mut self) -> Iter<'tx> {
        Iter::new(self.db)
    }
}

#[allow(unused)]
pub enum Iter<'tx> {
    UnInit(Database<'tx>),
    Init(Cursor<'tx>),
}

impl<'tx> Iter<'tx> {
    fn new(db: Database<'tx>) -> Self {
        Self::UnInit(db)
    }

    pub fn next(&mut self) -> crate::Result<Option<(&'tx crate::U32, &'tx str)>> {
        match self {
            Iter::UnInit(db) => {
                let mut cursor = db.cursor()?;
                match cursor.set_range(InternStringsKey::new(0.into()).as_bytes()) {
                    Ok(_) => {
                        *self = Iter::Init(cursor);
                        self.current()
                    }
                    Err(LmdbError::NOT_FOUND) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Iter::Init(cursor) => {
                cursor.next()?;
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
        let (key, value) = cursor.key_value()?;
        if key[0] != KEY_PREFIX_INTERN_STR {
            return Ok(None);
        }
        let hash = crate::U32::ref_from_bytes(&key[1..])
            .map_err(|_| crate::Error::InvalidMapping("intern string hash"))?;
        let string = unsafe { std::str::from_utf8_unchecked(value) };
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

    #[allow(unused)]
    pub fn parse(key: &[u8]) -> Option<&Self> {
        if let Ok(this) = Self::ref_from_bytes(key)
            && this.tag == KEY_PREFIX_INTERN_STR
        {
            return Some(this);
        }
        None
    }
}

#[allow(unused)]
pub struct Inspector<'tx> {
    db: Database<'tx>,
}

impl<'tx> Debug for Inspector<'tx> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_map();
        let mut cursor = self.db.cursor().map_err(|_| std::fmt::Error)?;
        let (mut key, mut value) =
            match cursor.set_range(InternStringsKey::new(0.into()).as_bytes()) {
                Ok(kv) => kv,
                Err(LmdbError::NOT_FOUND) => return s.finish(),
                Err(_) => return Err(std::fmt::Error),
            };
        while let Some(id) = InternStringsKey::parse(key) {
            s.key(&id.hash);
            let str = unsafe { std::str::from_utf8_unchecked(value) };
            s.value(&str);

            match cursor.next().optional().map_err(|_| std::fmt::Error)? {
                Some(kv) => {
                    key = kv.0;
                    value = kv.1;
                }
                None => break,
            }
        }
        s.finish()
    }
}
