use crate::id_set::IDSet;
use crate::read::{Decode, Decoder, ReadExt};
use crate::write::{Encode, Encoder, WriteExt};
use crate::Clock;
use crate::{ClientID, ID};
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasherDefault;
use std::iter::FromIterator;

/// State vector is a compact representation of all known blocks inserted and integrated into
/// a given document. This descriptor can be serialized and used to determine a difference between
/// seen and unseen inserts of two replicas of the same document, potentially existing in different
/// processes.
///
/// Another popular name for the concept represented by state vector is
/// [Version Vector](https://en.wikipedia.org/wiki/Version_vector).
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct StateVector(BTreeMap<ClientID, Clock>);

impl StateVector {
    /// Checks if current state vector contains any data.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns a number of unique clients observed by a document, current state vector corresponds
    /// to.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn new(map: BTreeMap<ClientID, Clock>) -> Self {
        StateVector(map)
    }

    /// Checks if current state vector includes given block identifier. Blocks, which identifiers
    /// can be found in a state vectors don't need to be encoded as part of an update, because they
    /// were already observed by their remote peer, current state vector refers to.
    pub fn contains(&self, id: &ID) -> bool {
        id.clock <= self.get(&id.client)
    }

    pub fn contains_client(&self, client_id: &ClientID) -> bool {
        self.0.contains_key(client_id)
    }

    /// Get the latest clock sequence number value for a given `client_id` as observed from
    /// the perspective of a current state vector.
    pub fn get(&self, client_id: &ClientID) -> Clock {
        match self.0.get(client_id) {
            Some(state) => *state,
            None => Clock::new(0),
        }
    }

    /// Updates a state vector observed clock sequence number for a given `client` by incrementing
    /// it by a given `delta`.
    pub fn inc_by(&mut self, client: ClientID, delta: Clock) -> Clock {
        let e = self.0.entry(client).or_default();
        if delta > 0 {
            *e = *e + delta;
        }
        *e
    }

    /// Updates a state vector observed clock sequence number for a given `client` by setting it to
    /// a minimum value between an already present one and the provided `clock`. In case if state
    /// vector didn't contain any value for that `client`, a `clock` value will be used.
    pub fn set_min(&mut self, client: ClientID, clock: Clock) {
        match self.0.entry(client) {
            Entry::Occupied(e) => {
                let value = e.into_mut();
                *value = (*value).min(clock);
            }
            Entry::Vacant(e) => {
                e.insert(clock);
            }
        }
    }

    /// Updates a state vector observed clock sequence number for a given `client` by setting it to
    /// a maximum value between an already present one and the provided `clock`. In case if state
    /// vector didn't contain any value for that `client`, a `clock` value will be used.
    pub fn set_max(&mut self, client: ClientID, clock: Clock) {
        let e = self.0.entry(client).or_default();
        *e = (*e).max(clock);
    }

    /// Returns an iterator which enables to traverse over all clients and their known clock values
    /// described by a current state vector.
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, ClientID, Clock> {
        self.0.iter()
    }

    /// Merges another state vector into a current one. Since vector's clock values can only be
    /// incremented, whenever a conflict between two states happen (both state vectors have
    /// different clock values for the same client entry), a highest of these to is considered to
    /// be the most up-to-date.
    pub fn merge(&mut self, other: Self) {
        for (client, clock) in other.0 {
            let e = self.0.entry(client).or_default();
            *e = (*e).max(clock);
        }
    }

    /// Updates current state vector by clock values from another state vector. If `other` doesn't
    /// have a given client, entry is retained. If `other` has a given client, entry is set to its
    /// value, if it's lesser than the current one. Otherwise, it is removed.
    ///
    /// This method is used to calculate the set of missing updates between two peers.
    pub fn clear_present(&self, other: &Self) -> Self {
        let mut diff = BTreeMap::new();
        for (client, &local_clock) in self.iter() {
            let remote_clock = other.get(client);
            if local_clock > remote_clock {
                diff.insert(*client, remote_clock);
            }
        }
        StateVector(diff)
    }
}

impl FromIterator<(ClientID, Clock)> for StateVector {
    fn from_iter<T: IntoIterator<Item = (ClientID, Clock)>>(iter: T) -> Self {
        StateVector::new(BTreeMap::from_iter(iter))
    }
}

impl Decode for StateVector {
    fn decode_with<D: Decoder>(decoder: &mut D) -> crate::Result<Self> {
        let len = decoder.read_var::<u32>()? as usize;
        let mut sv = BTreeMap::new();
        let mut i = 0;
        while i < len {
            let client = decoder.read_var()?;
            let clock = decoder.read_var()?;
            sv.insert(client, clock);
            i += 1;
        }
        Ok(StateVector(sv))
    }
}

impl Encode for StateVector {
    fn encode_with<E: Encoder>(&self, encoder: &mut E) -> crate::Result<()> {
        encoder.write_var(self.len())?;
        for (&client, &clock) in self.iter() {
            encoder.write_var(client)?;
            encoder.write_var(clock)?;
        }
        Ok(())
    }
}

impl PartialOrd for StateVector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut res = Some(Ordering::Equal);
        let mut a = self.0.iter();
        let mut b = other.0.iter();

        let mut ae = a.next();
        let mut be = b.next();

        loop {
            match (ae, be) {
                (None, None) => return res,
                (Some(_), None) => {
                    if res == Some(Ordering::Less) {
                        return None;
                    } else {
                        return res;
                    }
                }
                (None, Some(_)) => {
                    if res == Some(Ordering::Greater) {
                        return None;
                    } else {
                        return res;
                    }
                }
                (Some((ak, av)), Some((bk, bv))) => match ak.cmp(bk) {
                    Ordering::Equal => match av.get().cmp(&(*bv).get()) {
                        Ordering::Equal => {
                            ae = a.next();
                            be = b.next();
                        }
                        Ordering::Less if res == Some(Ordering::Greater) => {
                            return None;
                        }
                        Ordering::Greater if res == Some(Ordering::Less) => {
                            return None;
                        }
                        other => {
                            res = Some(other);
                            ae = a.next();
                            be = b.next();
                        }
                    },
                    Ordering::Less if res == Some(Ordering::Less) => {
                        return None;
                    }
                    Ordering::Less => {
                        res = Some(Ordering::Greater);
                        ae = a.next();
                    }
                    Ordering::Greater if res == Some(Ordering::Greater) => {
                        return None;
                    }
                    Ordering::Greater => {
                        res = Some(Ordering::Less);
                        be = b.next();
                    }
                },
            }
        }
    }
}

/// Snapshot describes a state of a document store at a given point in (logical) time. In practice
/// it's a combination of [StateVector] (a summary of all observed insert/update operations)
/// and a [DeleteSet] (a summary of all observed deletions).
#[derive(Default, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Compressed information about all deleted blocks at current snapshot time.
    pub delete_set: IDSet,
    /// Logical clock describing a current snapshot time.
    pub state_map: StateVector,
}

impl Snapshot {
    pub fn new(state_map: StateVector, delete_set: IDSet) -> Self {
        Snapshot {
            state_map,
            delete_set,
        }
    }

    pub(crate) fn is_visible(&self, id: &ID) -> bool {
        self.state_map.get(&id.client) > id.clock && !self.delete_set.contains(id)
    }
}

impl Encode for Snapshot {
    fn encode_with<E: Encoder>(&self, encoder: &mut E) -> crate::Result<()> {
        self.delete_set.encode_with(encoder)?;
        self.state_map.encode_with(encoder)
    }
}

impl Decode for Snapshot {
    fn decode_with<D: Decoder>(decoder: &mut D) -> crate::Result<Self> {
        let ds = IDSet::decode_with(decoder)?;
        let sm = StateVector::decode_with(decoder)?;
        Ok(Snapshot::new(sm, ds))
    }
}

#[cfg(test)]
mod test {
    use crate::{Clock, StateVector};
    use std::cmp::Ordering;
    use std::iter::FromIterator;

    #[test]
    fn ordering() {
        fn s<N: Into<Clock>>(a: N, b: N, c: N) -> StateVector {
            StateVector::from_iter([
                (1.into(), a.into()),
                (2.into(), b.into()),
                (3.into(), c.into()),
            ])
        }

        assert_eq!(s(1, 2, 3).partial_cmp(&s(1, 2, 3)), Some(Ordering::Equal));
        assert_eq!(s(1, 2, 2).partial_cmp(&s(1, 2, 3)), Some(Ordering::Less));
        assert_eq!(s(2, 2, 3).partial_cmp(&s(1, 2, 3)), Some(Ordering::Greater));
        assert_eq!(s(3, 2, 1).partial_cmp(&s(1, 2, 3)), None);
    }

    #[test]
    fn ordering_missing_fields() {
        let a = StateVector::from_iter([(1.into(), 1.into()), (2.into(), 2.into())]);
        let b = StateVector::from_iter([(2.into(), 1.into()), (3.into(), 2.into())]);
        assert_eq!(a.partial_cmp(&b), None);

        let a = StateVector::from_iter([(1.into(), 1.into()), (2.into(), 2.into())]);
        let b = StateVector::from_iter([
            (1.into(), 1.into()),
            (2.into(), 1.into()),
            (3.into(), 2.into()),
        ]);
        assert_eq!(a.partial_cmp(&b), None);

        let a = StateVector::from_iter([
            (1.into(), 1.into()),
            (2.into(), 2.into()),
            (3.into(), 3.into()),
        ]);
        let b = StateVector::from_iter([(2.into(), 2.into()), (3.into(), 3.into())]);
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));

        let a = StateVector::from_iter([(2.into(), 2.into()), (3.into(), 2.into())]);
        let b = StateVector::from_iter([
            (1.into(), 1.into()),
            (2.into(), 2.into()),
            (3.into(), 2.into()),
        ]);
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));

        let a = StateVector::default();
        let b = StateVector::default();
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));
    }
}
