# Content Store

## Entries schema

Each store occupies its own key-space within containing LMDB database, and for `ContentStore` the corresponding
key-space prefix byte is `0x03`.

- **key** is `0x03`-prefixed `ID` containing the clock range of blocks own content hold up by this entry.
- **value** is a content value owned by that block, matching its specific `ID` described by the key.

Effectively, only several types of content may require extra space in the `ContentStore` - the other should fit into
inline content of the `BlockHeader` structure:

- `ContentType::Json` and `ContentType::Atom` hold a user defined objects, serialized using either `serde_json` (for
  `ContentType::Json`) and `ysr::lib0` (for `ContentType::Atom`). Each `ContentStore` entry holds exactly a single
  serialized object. Therefore, a single block - which can own a multiple entries, if they were inserted sequentialy -
  can have a multiple corresponding entries in a `ContentStore`.
- `ContentType::String` is a slice of UTF-8 encoded text. For this content type, a block data will always fit into
  either inline content or be merged into a single entry in the `ContentStore` matching that corresponding block.
- `ContentType::Format` holds a zero-copied `FormattingAttribute` structure.
- `ContentType::Binary` holds a binary string.
- `ContentType::Embeded`, just like `ContentType::Atom` holds a single `ysr::lib0` serialized object.
- `ContentType::Doc` stores sub-document's `guid` string identifier.

If the user data fits into `BlockHeader`'s inline content field, it will be stored there instead of `ContentStore`. If
blocks are being merged:

- For `ContentType::Json` and `ContentType::Atom`, we always put the content in the `ContentStore` as long as the
  corresponding block has holds more than 1 element.
- For `ContentType::String` we concatenate two strings together and try to fit them into `BlockHeader`'s inline content.
  If the string gets too big, it will be instead moved into a `ContentStore` entry corresponding to that block.
- `ContentType::Deleted` and `ContentType::Node` never need to move their data into `ContentStore` entries, as they will
  always fit they data within the `BlockHeader`'s inline content.
- Other content types cannot be merged.