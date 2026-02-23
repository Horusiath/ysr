use crate::store::lmdb::store::KEY_PREFIX_STATE_VECTOR;
use crate::{ClientID, Clock, Optional, StateVector};
use lmdb_rs_m::{MdbError, MdbValue, ToMdbValue};
use std::collections::BTreeMap;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(transparent)]
pub struct StateVectorStore<'tx> {
    cursor: lmdb_rs_m::Cursor<'tx>,
}

impl<'tx> StateVectorStore<'tx> {
    pub fn new(cursor: lmdb_rs_m::Cursor<'tx>) -> Self {
        Self { cursor }
    }

    pub fn current(&mut self) -> crate::Result<Option<(&'tx ClientID, &'tx Clock)>> {
        let key: &'tx [u8] = self.cursor.get_key()?;
        if key[0] != KEY_PREFIX_STATE_VECTOR {
            return Ok(None);
        }

        let client: &'tx ClientID = ClientID::parse(&key[1..])?;
        let value: &'tx [u8] = self.cursor.get_value()?;
        let clock: &'tx Clock = Clock::ref_from_bytes(value)?;
        Ok(Some((client, clock)))
    }

    pub fn state_vector(&mut self) -> crate::Result<StateVector> {
        let mut buf = BTreeMap::new();

        let key = StateVectorKey::new(unsafe { ClientID::new_unchecked(0) });
        match self.cursor.to_gte_key(&key) {
            Ok(_) => {
                while let Some((&client, &clock)) = self.current()? {
                    buf.insert(client, clock);
                    if self.cursor.to_next_key().optional()?.is_none() {
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
        match self.cursor.to_key(&key) {
            Ok(_) => {
                let local_clock = *Clock::ref_from_bytes(self.cursor.get_value()?)?;
                if local_clock < clock {
                    self.cursor.replace(&value)?;
                }
                Ok(local_clock)
            }
            Err(MdbError::NotFound) => {
                self.cursor.set(&key, &value, 0)?;
                Ok(Clock::new(0))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn get_clock(&mut self, client_id: ClientID) -> crate::Result<Option<&'tx Clock>> {
        let key = StateVectorKey::new(client_id);
        match self.cursor.to_key(&key) {
            Ok(_) => {
                let value: &'tx [u8] = self.cursor.get_value()?;
                let clock = Clock::ref_from_bytes(value)?;
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
}

impl ToMdbValue for StateVectorKey {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        MdbValue::new_from_sized(self.as_bytes())
    }
}
