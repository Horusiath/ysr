# Meta Store

## Entries schema

Each store occupies its own key-space within containing LMDB database, and for `MetaStore` the corresponding
key-space prefix byte is `0x00`.

- **key** is `0x00`-prefixed `String` containing the document option.
- **value** is a byte string which holds a corresponding option's value. These values are serialized using `yrs::lib0`
  encoding.

## Known options

While user can store any predefined options, there are several defined by the `ysr` itself:

- `clientID` - holds a `ClientID` of the current client.