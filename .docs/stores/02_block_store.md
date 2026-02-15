# Block Store

## Entries schema

Each store occupies its own key-space within containing LMDB database, and for `BlockStore` the corresponding
key-space prefix byte is `0x02`.

- **key** is `0x02`-prefixed `ID` containing the block's own unique identifier.
- **value** is a `BlockHeader` containing the block metadata and (potentially) inlined content.