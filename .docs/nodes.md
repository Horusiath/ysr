# Node

`Node` is a collection living within a given `ysr` document, that can be edited collaboratively by multiple clients.
Its equivalent in `yjs` is called `YType`.

Each node has two capabilities:

- A **map** of attributes: you can insert or remove key-value entries into a node. Keys are of string type, while values
  can be any prelim type supported by `ysr` i.e. JSON- or lib0-encoded user data or other `Node` collections.
- A **list** of elements which works as an indexed sequence. It's used in several scenarios:
    - A regular list holding things like user data (serialized as JSON or lib0) or other `Node` collections.
    - A rich text structure, capable of holding user strings, formatting attributes or non-text embed data such as
      binaries or other `Node` collections.

There are two types of nodes:

1. **Root** nodes, which don't have any parent collection holding them. They are defined by user and identified by
   their string name. Once defined, they cannot be removed. They can also be defined on another client and initialized
   when integrating the blocks from the remote clients.
2. **Nested** nodes, which can be created as part of another insert operation into another `Node`. These are created and
   inserted as regular blocks and uniquely identified by the `ID` of the block they are wrapped in.

Unlike `yjs` or `yrs`, `ysr` doesn't have a dedicated structure to represent `Node` type. All node specific data is
spread or inlined as part of the block structure, while map entries are kept inside of supporting `MapEntryStore`.

Since blocks are identified by their `ID`, while **root nodes** are identified by user-defined strings, the following
mapping is done: whenever a new **root node** is defined, its string identifier is inserted
into [InternStringsStore](./stores/01_intern_strings_store.md), which returns a `Clock`-sized key hash of that string.
Since root nodes don't have a regular blocks, we create a virtual empty block holding that root node data. This block is
identified by an artificial `ID` created as `(ClientID::ROOT, {key-hash})` pair. `ClientID:ROOT` is a reserved value
used only by root nodes and not allowed to be used by the regular client.

Each node data is mapped onto block, where `NodeID` is an alias to its block `ID` and node data itself is placed
under `BlockHeader` content using `ContentType::Node` as differentiator. In order to access node capabilities:

- **map** capability data is stored within `MapEntryStore`. It contains entries in form of (`NodeId`, `String`) -> `ID`
  for each of the current `Node` entries.
- **list** capability requires only the `ID` of the first block which is an element of the current `Node` collection.
  This block contains one or more elements being at the beginning of the collection. Subsequent elements can be reached
  by moving to next blocks using `block.right` ID information of the next block.