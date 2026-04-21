use crate::id_set::IDRange;
use crate::lmdb::{Cursor, Database, Error as LmdbError};
use crate::{ClientID, Clock, ID};
use smallvec::SmallVec;
use std::ops::Range;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(transparent)]
pub struct DeleteSetStore<'tx> {
    db: Database<'tx>,
}

impl<'tx> DeleteSetStore<'tx> {
    pub const PREFIX: u8 = 0x06;

    pub fn new(db: Database<'tx>) -> DeleteSetStore<'tx> {
        DeleteSetStore { db }
    }

    pub fn contains(&self, id: &ID) -> crate::Result<bool> {
        let key = DeleteSetKey::new(id.client);
        match self.db.get(key.as_bytes()) {
            Ok(value) => {
                let ranges = Ranges::new(value);
                for range in ranges {
                    if range.contains(&id.clock) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Err(LmdbError::NOT_FOUND) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub fn merge<I: Iterator<Item = Range<Clock>>>(
        &self,
        client_id: ClientID,
        ranges: I,
    ) -> crate::Result<()> {
        let key = DeleteSetKey::new(client_id);
        match self.db.get(key.as_bytes()) {
            Ok(value) => {
                let current = Ranges::new(value);
                let merged = current.merge(ranges);
                self.db.put(key.as_bytes(), &merged)?;
            }
            Err(LmdbError::NOT_FOUND) => {
                let ranges = Ranges::create(ranges);
                self.db.put(key.as_bytes(), &ranges)?;
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    pub fn iter(&self) -> Iter<'_> {
        Iter::new(self.db)
    }

    pub fn delete_set(&self) -> crate::Result<crate::id_set::IDSet> {
        let mut set = crate::id_set::IDSet::default();
        for res in self.iter() {
            let (client_id, ranges) = res?;
            let id_range = IDRange::from(SmallVec::from_iter(ranges));
            set.insert_range(client_id, id_range);
        }
        Ok(set)
    }
}

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Debug, PartialEq, Eq)]
struct DeleteSetKey {
    tag: u8,
    client: ClientID,
}

impl DeleteSetKey {
    pub fn new(client: ClientID) -> Self {
        DeleteSetKey {
            tag: DeleteSetStore::PREFIX,
            client,
        }
    }

    fn parse(bytes: &[u8]) -> Option<&Self> {
        let key = DeleteSetKey::ref_from_bytes(bytes).ok()?;
        if key.tag == DeleteSetStore::PREFIX {
            Some(key)
        } else {
            None
        }
    }
}

pub struct Iter<'tx> {
    state: IterState<'tx>,
}

enum IterState<'tx> {
    Uninit(Database<'tx>),
    Init(Cursor<'tx>),
    Finished,
}

impl<'tx> Iter<'tx> {
    fn new(db: Database<'tx>) -> Iter<'tx> {
        Iter {
            state: IterState::Uninit(db),
        }
    }

    pub fn next(&mut self) -> crate::Result<Option<(ClientID, Ranges<'tx>)>> {
        let (k, v) = match &mut self.state {
            IterState::Uninit(db) => {
                let mut cursor = db.cursor()?;
                let kv = match cursor.set_range(&[DeleteSetStore::PREFIX]) {
                    Ok(kv) => kv,
                    Err(LmdbError::NOT_FOUND) => return self.finish(),
                    Err(e) => return Err(e.into()),
                };
                self.state = IterState::Init(cursor);
                kv
            }
            IterState::Init(cursor) => match cursor.next() {
                Ok(kv) => kv,
                Err(LmdbError::NOT_FOUND) => return self.finish(),
                Err(e) => return Err(e.into()),
            },
            IterState::Finished => return Ok(None),
        };
        match DeleteSetKey::parse(k) {
            Some(key) => {
                let ranges = Ranges::new(v);
                Ok(Some((key.client, ranges)))
            }
            None => self.finish(),
        }
    }

    fn finish(&mut self) -> crate::Result<Option<(ClientID, Ranges<'tx>)>> {
        self.state = IterState::Finished;
        Ok(None)
    }
}

impl<'tx> Iterator for Iter<'tx> {
    type Item = crate::Result<(ClientID, Ranges<'tx>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next() {
            Ok(None) => None,
            Ok(Some((client_id, ranges))) => Some(Ok((client_id, ranges))),
            Err(err) => Some(Err(err)),
        }
    }
}

/// A structured iterator over provided raw byte slice.
/// An internal slice contains sequence of ([Clock], [Clock]) pairs, describing start and end of
/// each range (both sides inclusive). These pairs are sorted by start value in ascending order.
pub struct Ranges<'tx> {
    buf: &'tx [u8],
}

impl<'tx> Ranges<'tx> {
    const CLOCK_SIZE: usize = size_of::<Clock>();
    pub fn new(buf: &'tx [u8]) -> Ranges<'tx> {
        Ranges { buf }
    }

    fn create<I: Iterator<Item = Range<Clock>>>(iter: I) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::CLOCK_SIZE * 2);
        for range in iter {
            buf.extend_from_slice(range.start.as_bytes());
            buf.extend_from_slice(range.end.as_bytes());
        }
        buf
    }

    /// Merges two ranges together, producing a [Range]-compatible byte array as a result.
    /// All overlapping ranges from current [Range] and the `other` are squashed together for more
    /// compact representation.
    pub fn merge<I: Iterator<Item = Range<Clock>>>(&self, other: I) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.buf.len() + 1);
        let mut iter_a = Ranges::new(self.buf);
        let mut iter_b = other;

        let mut a = iter_a.next();
        let mut b = iter_b.next();
        // track the current merged range as native u32 for easy arithmetic
        let mut current: Option<(u32, u32)> = None;

        loop {
            // pick the range with the smaller start
            let next = match (&a, &b) {
                (Some(range_a), Some(range_b)) => {
                    if range_a.start <= range_b.start {
                        let r = a.take();
                        a = iter_a.next();
                        r
                    } else {
                        let r = b.take();
                        b = iter_b.next();
                        r
                    }
                }
                (Some(_), None) => {
                    let range = a.take();
                    a = iter_a.next();
                    range
                }
                (None, Some(_)) => {
                    let range = b.take();
                    b = iter_b.next();
                    range
                }
                (None, None) => break,
            };
            let next = next.unwrap();
            let next_start = next.start.get();
            let next_end = next.end.get();

            match &mut current {
                Some((cs, ce)) => {
                    if next_start <= ce.saturating_add(1) {
                        // overlapping or adjacent — extend
                        *ce = (*ce).max(next_end);
                    } else {
                        // gap — flush current range
                        buf.extend_from_slice(Clock::new(*cs).as_bytes());
                        buf.extend_from_slice(Clock::new(*ce).as_bytes());
                        *cs = next_start;
                        *ce = next_end;
                    }
                }
                None => {
                    current = Some((next_start, next_end));
                }
            }
        }

        // flush the last range
        if let Some((cs, ce)) = current {
            buf.extend_from_slice(Clock::new(cs).as_bytes());
            buf.extend_from_slice(Clock::new(ce).as_bytes());
        }

        buf
    }
}

impl<'tx> Iterator for Ranges<'tx> {
    type Item = Range<Clock>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.len() < Self::CLOCK_SIZE * 2 {
            return None;
        }

        let start = *Clock::ref_from_bytes(&self.buf[0..Self::CLOCK_SIZE]).ok()?;
        let end =
            *Clock::ref_from_bytes(&self.buf[Self::CLOCK_SIZE..(Self::CLOCK_SIZE * 2)]).ok()?;
        self.buf = &self.buf[Self::CLOCK_SIZE * 2..];
        Some(start..end)
    }
}
