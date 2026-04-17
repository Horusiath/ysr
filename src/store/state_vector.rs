use crate::lmdb::{Database, Error as LmdbError};
use crate::store::KEY_PREFIX_STATE_VECTOR;
use crate::{ClientID, Clock, Optional, StateVector};
use std::collections::BTreeMap;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(transparent)]
pub struct StateVectorStore<'tx> {
    db: Database<'tx>,
}

impl<'tx> StateVectorStore<'tx> {
    pub fn new(db: Database<'tx>) -> Self {
        Self { db }
    }

    pub fn state_vector(&mut self) -> crate::Result<StateVector> {
        let mut buf = BTreeMap::new();
        let mut cursor = self.db.cursor()?;

        let key = StateVectorKey::new(unsafe { ClientID::new_unchecked(0) });
        match cursor.set_range(key.as_bytes()) {
            Ok(_) => {
                while let Some(key) = StateVectorKey::parse(cursor.key()?) {
                    let clock = *Clock::ref_from_bytes(cursor.value()?)
                        .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                    buf.insert(key.client, clock);
                    if cursor.next().optional()?.is_none() {
                        break;
                    }
                }
                Ok(StateVector::new(buf))
            }
            Err(LmdbError::NOT_FOUND) => Ok(StateVector::new(buf)),
            Err(e) => Err(crate::Error::from(e)),
        }
    }

    pub fn update(&mut self, client: ClientID, clock: Clock) -> crate::Result<Clock> {
        let key = StateVectorKey::new(client);
        let value = clock.as_bytes();
        let mut cursor = self.db.cursor()?;
        match cursor.set_key(key.as_bytes()) {
            Ok(_) => {
                let local_clock = *Clock::ref_from_bytes(cursor.value()?)
                    .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                if local_clock < clock {
                    cursor.put_current(value)?;
                }
                Ok(local_clock)
            }
            Err(LmdbError::NOT_FOUND) => {
                cursor.put(key.as_bytes(), value, 0)?;
                Ok(Clock::new(0))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn get_clock(&mut self, client_id: ClientID) -> crate::Result<Option<&'tx Clock>> {
        let key = StateVectorKey::new(client_id);
        match self.db.get(key.as_bytes()) {
            Ok(value) => {
                let clock = Clock::ref_from_bytes(value)
                    .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                Ok(Some(clock))
            }
            Err(LmdbError::NOT_FOUND) => Ok(None),
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
