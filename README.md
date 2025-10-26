# ysr

Ysr (read: *wiser*) is a Conflict-Free Data Type structure implemented over persistent key-value storage. The algorithm
itself is based on [yrs](https://docs.rs/yrs/latest/yrs/) (read: *wires*) and [yjs](https://yjs.dev) libraries
and implements a [YATA](https://www.bartoszsypytkowski.com/yata/) conflict resolution.

Our goal is to provide a library that has a persistence built into the core primitives. This should help in several
areas:

1. Usually for long living documents persistence is non-negotiable. However, if the data type doesn't support it
   natively, it means that we need to deserialize entire document just to merge a single incremental update (like user's
   keystroke). This brings a burden of building entire persistence layer in a way that supports incremental update
   appends with periodic compaction. With persistence built-in, we can operate on the document without expensive
   serialization/deserialization.
2. Over time documents tend to grow in size, not only because of the user input, but also because of growing document
   history (used for things like snapshotting) and metadata associated with it (used for conflict resolution). This
   causes memory to grow which can be devastating in some scenarios (like operating thousands of the documents on the
   web server). While *yrs* and *yjs* are great at compacting document size, it comes at a cost of features like
   snapshots. With persistent on-disk representation, we can operate only on the fragments of the document that are
   directly related to the operations required by the API in use.
3. Once a user data grows over certain size, its no longer feasible to keep it in memory as a single document. This
   requires developers to design a partitioning scheme, which - as we noticed - is a constant source of issues.
   On-disk representation doesn't have to care about memory limits.
4. Since our representation is backed by LMDB (a persistent ACID-compliant key-value store), all changes are atomic.
   This means that malicious or otherwise undesired updates can be rolled back without affecting the document data
   structure. With *yrs*/*yjs* this would require reloading the whole document back from the persistent memory.

**This work is still unstable and on-disk representation is sure to change!**

## Progress

- [x] lib0 v1 deserialization.
- [ ] lib0 v2 deserialization.
- [x] update application with conflict resolution
    - [ ] support for move operations
    - [ ] support for weak links
- [x] block split/merge algorithms
- [ ] update generation
- [ ] collaborative Map
    - [x] core API implemented
    - [x] tests (code only)
- [ ] collaborative List
    - [ ] core API implemented
    - [x] tests (code only)
- [ ] collaborative Text
    - [ ] core API implemented
    - [x] tests (code only)
- [ ] collaborative XmlElement/XmlFragment/XmlText
    - [ ] core API implemented
    - [ ] tests
- [ ] Subdocuments

## Sponsors

[![NLNET](https://nlnet.nl/image/logo_nlnet.svg)](https://nlnet.nl/)