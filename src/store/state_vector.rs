use crate::store::KEY_PREFIX_STATE_VECTOR;
use crate::{ClientID, Clock, Optional, StateVector};
use lmdb_rs_m::{MdbError, MdbValue, ToMdbValue};
use std::collections::BTreeMap;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(transparent)]
pub struct StateVectorStore<'tx> {
    db: &'tx lmdb_rs_m::Database<'tx>,
}

impl<'tx> StateVectorStore<'tx> {
    pub fn new(db: &'tx lmdb_rs_m::Database<'tx>) -> Self {
        Self { db }
    }

    pub fn state_vector(&mut self) -> crate::Result<StateVector> {
        let mut buf = BTreeMap::new();
        let mut cursor = self.db.new_cursor()?;

        let key = StateVectorKey::new(unsafe { ClientID::new_unchecked(0) });
        match cursor.to_gte_key(&key) {
            Ok(_) => {
                while let Some(key) = StateVectorKey::parse(cursor.get_key()?) {
                    let clock = *Clock::ref_from_bytes(cursor.get_value()?)
                        .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                    buf.insert(key.client, clock);
                    if cursor.to_next_key().optional()?.is_none() {
                        break;
                    }
                }
                Ok(StateVector::new(buf))
            }
            Err(MdbError::NotFound) => Ok(StateVector::new(buf)),
            Err(e) => Err(crate::Error::from(e)),
        }
    }

    pub fn update(&mut self, client: ClientID, clock: Clock) -> crate::Result<Clock> {
        let key = StateVectorKey::new(client);
        let value = clock.as_bytes();
        let mut cursor = self.db.new_cursor()?;
        match cursor.to_key(&key) {
            Ok(_) => {
                let local_clock = *Clock::ref_from_bytes(cursor.get_value()?)
                    .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                if local_clock < clock {
                    cursor.replace(&value)?;
                }
                Ok(local_clock)
            }
            Err(MdbError::NotFound) => {
                cursor.set(&key, &value, 0)?;
                Ok(Clock::new(0))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn get_clock(&mut self, client_id: ClientID) -> crate::Result<Option<&'tx Clock>> {
        let key = StateVectorKey::new(client_id);
        match self.db.get(&key) {
            Ok(value) => {
                let clock = Clock::ref_from_bytes(value)
                    .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                Ok(Some(clock))
            }
            Err(MdbError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateVectorKey {
    tag: u8,
    client: ClientID,
}

impl StateVectorKey {
    pub fn new(client: ClientID) -> Self {
        StateVectorKey {
            tag: KEY_PREFIX_STATE_VECTOR,
            client,
        }
    }

    pub fn parse(data: &[u8]) -> Option<&Self> {
        let key = Self::ref_from_bytes(data).ok()?;
        if key.tag == KEY_PREFIX_STATE_VECTOR {
            Some(key)
        } else {
            None
        }
    }
}

impl ToMdbValue for StateVectorKey {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        let slice = self.as_bytes();
        let ptr = slice.as_ptr() as *const _;
        unsafe { MdbValue::new(ptr, slice.len()) }
    }
}
