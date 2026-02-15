# Intern Strings Store

An `InternStringsStore` is a dedicated storage used for storing interned strings, that can be reused in-between various
`ysr` components, such as:

- Root-level nodes, which are referred to using their user-defined string names rather than block IDs like nested nodes
  do.

`InternStringsStore` is designed to only store a handful of elements.

## Entries scheme

Each store occupies its own key-space within containing LMDB database, and for `InternStringsStore` the corresponding
key-space prefix byte for all the keys it's managing is `0x01`.

All entries in `InternStringsStore` conform to the same schema:

- **key** is `0x01`-prefixed big endian unsinged 32-bit integer holding a XX hash of the interned string.
- **value** is a string itself.

DISCLAIMER: *since we operate on rather small hashes it's important to only use a small number of strings
here, another is that, once inserted intern strings are never deleted*. Whenever we try to insert a new value under
given hash, and it turns out that there existed a different value under the same hash, and error should be returned.
