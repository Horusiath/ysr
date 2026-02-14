## Blocks

A central unit of work in `ysr` architecture is a structure called `BlockHeader`. It's a zero-copy, fixed size structure
that stores a metadata required to potential conflict resolution.

The `ID` is not stored within the `BlockHeader`, but it's part of `Block` and `BlockMut` structures. The purpose of
`BlockHeader` is to be stored together with its `ID` as an `ID`->`BlockHeader` key-value pair inside of LMDB. These are
stored in a dedicated key-space, prefixed by `KEY_PREFIX_BLOCK` byte. Usually when reading or writing an entry in that
key-space, we use a `BlockKey` data structure. It's implementing an `ToMdbValue` trait, and therefore it can be used
directly with [lmdb-rs-m](https://docs.rs/crate/lmdb-rs-m/0.8.1/source/) API.

Each time a new element is being inserted a new block needs to be created and a new `ID` is being generated for that
clock. Sometimes we might want to insert more than one element per block: as long as elements are inserted by the same
client, sequentially, within the same collection scope, they can share the same block. A total number of elements stored
within such block is present in a `clock_len` field - only a handful of supported content types can be stored together
within the same block boundaries:

- `CONTENT_TYPE_ATOM` and `CONTENT_TYPE_JSON` are both used for storing user-defined structures stores as either lib0
  encoding or [serde_json](https://docs.rs/serde_json/1.0.149/serde_json/) format. In such case `clock_len` describes a
  number of objects stored.
- `CONTENT_TYPE_STRING` describes a user-entered characters, edited for purpose of collaborative text editing. For the
  compatibility with `yjs`, a `clock_len` here has to describe a number of UTF-16 characters.
- `CONTENT_TYPE_DELETED` uses `clock_len` to describe a number of deleted elements.
- `CONTENT_TYPE_GC` and `CONTENT_TYPE_SKIP` also define numer of garbage collected or skipped elements respectively,
  however they are not stored inside of LMDB and are used only for the purpose of serialization and interfacing with
  `yjs`.

Next field is `flags`, which stores a `BlockFlags` bit-flag field keeping the information about various properties of
the containing `BlockHeader`:

- If `INLINE_CONTENT` bit is set, it means that a blocks content is small enough to fit into `BlockHeader.content`
  field. Otherwise, it's stored in a separate LMDB key-space dedicated specifically for content (and prefixed with
  `KEY_PREFIX_CONTENT` prefix byte).
- If `COUNTABLE` bit is set, it means that the block holds the data that should be counted for methods such as length
  computation. All specific content types that goes into this category can be seen in `ContentType::is_countable`
  method.
- If `DELETED` bit is set, it means that the current block has been deleted. It doesn't always require
  `ContentType.Deleted` enum to be present. When used as a tombstone, `BlockHeader` can have a `DELETED` bit flag set,
  but still retain its original content for purposes such as time travel state resolution or future undo/redo
  operations.
- If `LEFT` bit is set, it means that `BlockHeader.left` field contains an `ID` pointing to a left block neighbor of a
  current block.
- If `RIGHT` bit is set, it means that `BlockHeader.right` field contains an `ID` pointing to a right block neighbor of
  a current block.
- If `ORIGIN_LEFT` bit is set, it means that `BlockHeader.origin_left` field contains an `ID` pointing to a block, that
  was a left-side neighbor of a current block at the moment of current block's insertion.
- If `ORIGIN_RIGHT` bit is set, it means that `BlockHeader.origin_right` field contains an `ID` pointing to a block,
  that was a right-side neighbor of a current block at the moment of current block's insertion.

## Merging blocks together

## Splitting blocks

## Block Range

`BlockRange`