# State Vector Store

`StateVectorStore` is responsible for storing the [StateVector](../state_vector.md) of the current document.

## Entries schema

Each store occupies its own key-space within containing LMDB database, and for `StateVectorStore` the corresponding
key-space prefix byte is `0x05`.

- **key** is `0x05`-prefixed `ClientID`.
- **value** is `Clock` for that given `ClientID`.
