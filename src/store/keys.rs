use crate::store::AsKey;
use crate::{ClientID, Clock};
use zerocopy::FromBytes;

pub const STATE_VECTOR_KEY: &[u8] = &[1];

pub struct StateVectorKey;

impl AsKey for StateVectorKey {
    type Key = ClientID;
    type Value = Clock;

    fn as_key(&self) -> &[u8] {
        &STATE_VECTOR_KEY
    }

    fn parse_key(key: &[u8]) -> Option<&Self::Key> {
        let (_prefix, client_id) = ClientID::ref_from_suffix(&key).ok()?;
        Some(client_id)
    }

    fn parse_value(value: &[u8]) -> Option<&Self::Value> {
        let clock = Clock::ref_from_bytes(&value).ok()?;
        Some(clock)
    }
}
