use crate::block::ID;
use crate::read::{Decode, Decoder, ReadExt};
use crate::write::{Encode, Encoder, WriteExt};
use crate::{ClientID, Clock};
use smallvec::{SmallVec, smallvec};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::ops::Range;

/// IDSet is a temporary object that is created when needed.
/// - When created in a transaction, it must only be accessed after sorting and merging.
///   - This IDSet is sent to other clients.
/// - We do not create a IDSet when we send a sync message. The IDSet message is created
///   directly from StructStore.
/// - We read a IDSet as apart from sync/update message. In this case the IDSet is already
///   sorted and merged.
#[derive(Default, Clone, PartialEq, Eq)]
pub struct IDSet(BTreeMap<ClientID, IDRange>);

impl IDSet {
    /// Returns number of clients stored;
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn ranges(&self) -> Ranges<'_> {
        self.0.iter()
    }

    /// Check if current [IdSet] contains given `id`.
    pub fn contains(&self, id: &ID) -> bool {
        if let Some(ranges) = self.0.get(&id.client) {
            ranges.contains(&id.clock)
        } else {
            false
        }
    }

    /// Checks if current ID set contains any data.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty() || self.0.values().all(|r| r.is_empty())
    }

    /// Compacts an internal ranges representation.
    pub fn squash(&mut self) {
        for block in self.0.values_mut() {
            block.squash();
        }
    }

    pub fn insert(&mut self, id: ID, len: Clock) {
        let range = id.clock..(id.clock + len);
        match self.0.entry(id.client) {
            Entry::Occupied(r) => {
                r.into_mut().push(range);
            }
            Entry::Vacant(e) => {
                e.insert(IDRange::from(smallvec![range]));
            }
        }
    }

    /// Inserts a new ID `range` corresponding with a given `client`.
    pub fn insert_range(&mut self, client: ClientID, range: IDRange) {
        self.0.insert(client, range);
    }

    /// Merges another ID set into a current one, combining their information about observed ID
    /// ranges and squashing them if necessary.
    pub fn merge(&mut self, other: Self) {
        other.0.into_iter().for_each(|(client, range)| {
            if let Some(r) = self.0.get_mut(&client) {
                r.merge(range)
            } else {
                self.0.insert(client, range);
            }
        });
        self.squash()
    }

    pub fn get(&self, client_id: &ClientID) -> Option<&IDRange> {
        self.0.get(client_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ClientID, &IDRange)> {
        self.0.iter()
    }
}

impl Encode for IDSet {
    fn encode_with<E: Encoder>(&self, encoder: &mut E) -> crate::Result<()> {
        encoder.write_var(self.0.len() as u32)?;
        for (&client_id, range) in self.0.iter() {
            encoder.reset_ds_cur_val();
            encoder.write_var(client_id)?;
            range.encode_with(encoder)?;
        }
        Ok(())
    }
}

impl Decode for IDSet {
    fn decode_with<D: Decoder>(decoder: &mut D) -> crate::Result<Self> {
        let mut set = Self::default();
        let client_len: u32 = decoder.read_var()?;
        let mut i = 0;
        while i < client_len {
            decoder.reset_ds_cur_val();
            let client: ClientID = decoder.read_var()?;
            let range = IDRange::decode_with(decoder)?;
            set.0.insert(client, range);
            i += 1;
        }
        Ok(set)
    }
}

pub(crate) type Ranges<'a> = std::collections::btree_map::Iter<'a, ClientID, IDRange>;

/// [IDRange] describes a single space of an [ID] clock values, belonging to the same client.
/// It can contain from a single continuous space, or multiple ones having "holes" between them.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IDRange(SmallVec<[Range<Clock>; 1]>);

impl IDRange {
    pub fn with_capacity(capacity: usize) -> Self {
        IDRange(SmallVec::with_capacity(capacity))
    }

    /// Check if range is empty (doesn't cover any clock space).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Inverts current [IDRange], returning another [IDRange] that contains all
    /// "holes" (ranges not included in current range). If current range is a continuous space
    /// starting from the initial clock (eg. [0..5)), then returned range will be empty.
    pub fn invert(&self) -> Self {
        let mut inv = SmallVec::new();
        let mut start: Clock = 0.into();
        for range in self.0.iter() {
            if range.start > start {
                inv.push(start..range.start);
            }
            start = range.end;
        }
        IDRange(inv)
    }

    /// Check if given clock exists within current [IDRange].
    pub fn contains(&self, clock: &Clock) -> bool {
        self.0.iter().any(|r| r.contains(clock))
    }

    /// Iterate over ranges described by current [IDRange].
    pub fn iter(&self) -> std::slice::Iter<'_, Range<Clock>> {
        self.0.iter()
    }

    fn push(&mut self, range: Range<Clock>) {
        if self.0.is_empty() {
            self.0.push(range);
        } else {
            let last_idx = self.0.len() - 1;
            let last = &mut self.0[last_idx];
            if !Self::try_join(last, &range) {
                self.0.push(range);
            }
        }
    }

    /// Alters current [IDRange] by compacting its internal implementation (in fragmented case).
    /// Example: fragmented space of [0,3), [3,5), [6,7) will be compacted into [0,5), [6,7).
    fn squash(&mut self) {
        let ranges = &mut self.0;
        if !ranges.is_empty() {
            ranges.sort_by(|a, b| a.start.cmp(&b.start));
            let mut new_len = 1;

            let len = ranges.len() as isize;
            let head = ranges.as_mut_ptr();
            let mut current = unsafe { head.as_mut().unwrap() };
            let mut i = 1;
            while i < len {
                let next = unsafe { head.offset(i).as_ref().unwrap() };
                if !Self::try_join(current, next) {
                    // current and next are disjoined eg. [0,5) & [6,9)

                    // move current pointer one index to the left: by using new_len we
                    // squash ranges possibly already merged to current
                    current = unsafe { head.offset(new_len).as_mut().unwrap() };

                    // make next a new current
                    current.start = next.start;
                    current.end = next.end;
                    new_len += 1;
                }

                i += 1;
            }

            if new_len == 1 {
                self.0 = smallvec![ranges[0].clone()];
            } else if ranges.len() != new_len as usize {
                ranges.truncate(new_len as usize);
            }
        }
    }

    fn is_squashed(&self) -> bool {
        let mut i = self.0.iter();
        if let Some(r) = i.next() {
            let mut prev_start = r.start;
            let mut prev_end = r.end;
            for r in i {
                if r.start < prev_end {
                    return false;
                }
                prev_start = r.start;
                prev_end = r.end;
            }
            true
        } else {
            true
        }
    }

    fn merge(&mut self, mut other: IDRange) {
        self.0.append(&mut other.0);
    }

    fn encode_raw<E: Encoder>(&self, encoder: &mut E) -> crate::Result<()> {
        encoder.write_var(self.0.len() as u64)?;
        for range in self.iter() {
            range.encode_with(encoder)?;
        }
        Ok(())
    }

    #[inline]
    fn try_join(a: &mut Range<Clock>, b: &Range<Clock>) -> bool {
        if Self::disjoint(a, b) {
            false
        } else {
            a.start = a.start.min(b.start);
            a.end = a.end.max(b.end);
            true
        }
    }

    #[inline]
    fn disjoint(a: &Range<Clock>, b: &Range<Clock>) -> bool {
        a.start > b.end || b.start > a.end
    }
}

impl From<SmallVec<[Range<Clock>; 1]>> for IDRange {
    #[inline]
    fn from(value: SmallVec<[Range<Clock>; 1]>) -> Self {
        IDRange(value)
    }
}

impl Encode for IDRange {
    fn encode_with<E: Encoder>(&self, encoder: &mut E) -> crate::Result<()> {
        if self.is_squashed() {
            self.encode_raw(encoder)?;
        } else {
            let mut clone = self.clone();
            clone.squash();
            clone.encode_raw(encoder)?;
        }
        Ok(())
    }
}

impl Decode for IDRange {
    fn decode_with<D: Decoder>(decoder: &mut D) -> crate::Result<Self> {
        let len = decoder.read_var::<u32>()?;
        let mut ranges = SmallVec::with_capacity(len as usize);
        let mut i = 0;
        while i < len {
            ranges.push(Range::decode_with(decoder)?);
            i += 1;
        }
        Ok(IDRange(ranges))
    }
}
