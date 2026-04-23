use crate::lmdb::{Database, Error as LmdbError};
use crate::store::KEY_PREFIX_STATE_VECTOR;
use crate::{ClientID, Clock, StateVector};
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
        let (mut k, mut v) = match cursor.set_range(key.as_bytes()) {
            Ok(kv) => kv,
            Err(LmdbError::NOT_FOUND) => return Ok(StateVector::new(buf)),
            Err(e) => return Err(crate::Error::from(e)),
        };
        loop {
            match StateVectorKey::parse(k) {
                Some(key) => {
                    let clock = *Clock::ref_from_bytes(v)
                        .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                    buf.insert(key.client, clock);
                }
                None => break,
            }
            match cursor.next() {
                Ok(kv) => {
                    k = kv.0;
                    v = kv.1;
                }
                Err(LmdbError::NOT_FOUND) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(StateVector::new(buf))
    }

    pub fn update(&mut self, client: ClientID, clock: Clock) -> crate::Result<Clock> {
        let key = StateVectorKey::new(client);
        let key_bytes = key.as_bytes();
        let value = clock.as_bytes();
        let mut cursor = self.db.cursor()?;
        match cursor.set_key(key_bytes) {
            Ok((_, current_value)) => {
                let local_clock = *Clock::ref_from_bytes(current_value)
                    .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                if local_clock < clock {
                    cursor.put_current(key_bytes, value)?;
                }
                Ok(local_clock)
            }
            Err(LmdbError::NOT_FOUND) => {
                cursor.put(key_bytes, value, 0)?;
                Ok(Clock::new(0))
            }
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
