use crate::block::ID;
use std::borrow::Cow;
use zerocopy::{FromBytes, IntoBytes};

/// Hash map copy-on-write bucket, used by LMDB entries responsible for hosting [crate::Map] key-value pairs.
#[repr(transparent)]
pub struct Bucket<'a> {
    entries: Cow<'a, [u8]>,
}

impl<'a> Bucket<'a> {
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        Bucket {
            entries: Cow::Borrowed(bytes),
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.entries
    }

    pub fn get(&self, key: &[u8]) -> Option<&ID> {
        for (curr, value) in self.iter() {
            if curr == key {
                return Some(value);
            }
        }
        None
    }

    pub fn iter(&self) -> BucketIter<'_> {
        BucketIter {
            data: &self.entries,
        }
    }

    pub fn iter_mut(&mut self) -> BucketIterMut<'_> {
        BucketIterMut {
            data: self.entries.to_mut(),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &ID) -> bool {
        debug_assert!(key.len() <= 255);

        for (curr, existing) in self.iter_mut() {
            if curr == key {
                *existing = *value;
                return false;
            }
        }
        // this key does not exist yet, we push a new entry to the end
        let mut owned = self.entries.to_vec();
        owned.push(key.len() as u8);
        owned.extend_from_slice(key);
        owned.extend_from_slice(value.as_bytes());
        self.entries = Cow::Owned(owned);
        true
    }

    /// Replaces the first occurrence of `prev_value` with `value`.
    /// Returns the key associated with the replaced value, if any.
    pub fn replace_value(&mut self, prev_value: &ID, value: &ID) -> Option<&[u8]> {
        for (key, existing) in self.iter_mut() {
            if *existing == *prev_value {
                *existing = *value;
                return Some(key);
            }
        }
        None
    }

    pub fn remove(&mut self, key: &[u8]) -> bool {
        let mut offset = 0;
        for (curr, _) in self.iter() {
            let entry_len = 1 + curr.len() + ID_SIZE;
            if curr == key {
                let mut owned = self.entries.to_vec();
                owned.drain(offset..offset + entry_len);
                self.entries = Cow::Owned(owned);
                return true;
            }
            offset += entry_len;
        }
        false
    }
}

const ID_SIZE: usize = size_of::<ID>();

#[repr(transparent)]
pub struct BucketIter<'a> {
    data: &'a [u8],
}

impl<'a> Iterator for BucketIter<'a> {
    type Item = (&'a [u8], &'a ID);

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }
        let key_len: usize = self.data[0] as usize;
        if self.data.len() < 1 + key_len + ID_SIZE {
            return None;
        }
        let key = &self.data[1..1 + key_len];
        let value = &self.data[1 + key_len..1 + key_len + ID_SIZE];
        let value = ID::ref_from_bytes(value).unwrap();
        self.data = &self.data[1 + key_len + ID_SIZE..];
        Some((key, value))
    }
}

#[repr(transparent)]
pub struct BucketIterMut<'a> {
    data: &'a mut [u8],
}

impl<'a> Iterator for BucketIterMut<'a> {
    type Item = (&'a [u8], &'a mut ID);

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }
        let key_len: usize = self.data[0] as usize;
        if self.data.len() < 1 + key_len + ID_SIZE {
            return None;
        }
        let data: &'a mut [u8] = std::mem::take(&mut self.data);
        let (key, rest) = data.split_at_mut(1 + key_len);

        let key: &'a [u8] = &key[1..];
        let (value, rest) = rest.split_at_mut(ID_SIZE);
        let value = ID::mut_from_bytes(value).unwrap();
        self.data = rest;
        Some((key, value))
    }
}

#[cfg(test)]
mod test {
    use super::Bucket;
    use crate::block::ID;

    #[test]
    fn bucket_operations() {
        let mut bucket = Bucket::from_bytes(&[]);

        // insert new value into empty bucket
        let v1 = ID::new(1.into(), 0.into());
        bucket.insert(b"key-1", &v1);
        let value = bucket.get(b"key-1");
        assert_eq!(value, Some(&v1));

        // insert value into non-empty bucket
        let v2 = ID::new(1.into(), 1.into());
        bucket.insert(b"key-2", &v2);
        let value = bucket.get(b"key-2");
        assert_eq!(value, Some(&v2));

        // replace value for existing key
        let v3 = ID::new(1.into(), 2.into());
        bucket.insert(b"key-1", &v3);
        let value = bucket.get(b"key-1");
        assert_eq!(value, Some(&v3));

        // get non-existing key
        let value = bucket.get(b"key-3");
        assert_eq!(value, None);

        // remove existing key
        let removed = bucket.remove(b"key-1");
        assert!(removed);
        let value = bucket.get(b"key-1");
        assert_eq!(value, None);
        let value = bucket.get(b"key-2");
        assert_eq!(value, Some(&v2)); // ensure other key is still there

        // remove non-existing key
        let removed = bucket.remove(b"key-3");
        assert!(!removed);
        let value = bucket.get(b"key-2");
        assert_eq!(value, Some(&v2)); // ensure non-removed key is still there
    }
}
