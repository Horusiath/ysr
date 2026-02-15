# Map Entry Store

## Entries schema

Each store occupies its own key-space within containing LMDB database, and for `MapEntryStore` the corresponding
key-space prefix byte for all the keys it's managing is `0x04`.

- **key** is `0x04`-prefixed (`NodeId`, `String`) pair. String is a user-defined text used by the map operators of
  `Node` API.
- **value** is an `ID` of the block holding the current user-defined value corresponding to that inserted entry. Keep in
  mind that the corresponding block may have been deleted.

