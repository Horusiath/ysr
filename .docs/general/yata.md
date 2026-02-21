# YATA conflict resolution algorithm

This document explains the conflict resolution algorithm used by `yjs`, `yrs` and `ysr` libraries.

*YATA* conflict resolution is designed to establish a total order in the indexed sequence of elements, edited
collaboratively. Under conditions of multiple clients editing the same logical structure - replicated on each client -
each of these clients can operate on the document in the multi-master mode. It means that each client is able to add new
elements to the sequence without prior coordination with other clients. Under such circumstances it's possible, that the
updates made by individual clients will be exchanged and applied of each of the clients replicated document in different
order. The purpose of *YATA* is to guarantee that in such situation the output of each client document will remain the
same.

To correctly order inserted elements, each one of them receives an unique [ID](../id.md) once it's integrated into the
document's structure. To correctly establish an order, when inserted, each element is wrapped within a `Block`
structure, which attaches a necessary metadata. From the conflict resolution perspective the most important fields are:

- `id` of the inserted block itself.
- `origin_left` which describes `ID` of the block placed before current one at the moment of insertion. If current block
  is the first one in a sequence, this value is `None`.
- `origin_right` which describes `ID` of the block placed after current one at the moment of insertion. If current block
  is the last one in a sequence, this value is `None`.

Additionally, we have keep two extra fields:

- `left` which describes `ID` of the block placed before current one at the present moment.
- `right` which describes `ID` of the block placed after current one at the present moment.

Unlike `origin_left`/`origin_right` - which are immutable as they describe fact of the past - `left` and `right` fields
reflect a current state of relationship between blocks in the sequence.

To detect that conflict with another concurrently inserted block has occurred, when integrating the block we need to
check for its left and right origin. In non-concurrent case, the `origin_left.right` is equal to `origin_right` and vice
versa: `origin_right.left` is equal to `origin_left`. Most of the time just a single comparison
`origin_left.right != origin_right` is enough to already recognize conflict occurrence, but in case when `origin_left`
is `None`, we might also need to check if `origin_right.left != None`

**Conflict resolution** is a matter of correcting `left` and `right` neighbours to establish a common total order of
blocks regardless of the order of insertion. //TODO

## Block merging

Wrapping every individual inserted element into block can be expensive. In order to optimize that process, *YATA*
enables merging multiple blocks together into one block, representing multiple elements inserted sequentially by the
same client.

The prerequisite required to merge two blocks `x` and `y` together is that:

1. They must be produced by the same client (`x.id.client == y.id.client`).
2. They must be neighbors, meaning `y` was inserted on the right side of `x` (`x.right == y.id`).
3. They must be inserted one after another - no other block coming from the same client must have been integrated in the
   meantime. We can detect that by comparing their clocks, which each client increments sequentially whenever they are
   about to insert an element (`x.id.clock + x.clock_len() == y.id.clock`).
4. They were originally inserted one after another (
   `y.origin_left() == x.last_id() && x.origin_right() == y.origin_right()`).
5. Deleted blocks cannot be merged with non-deleted ones (`x.is_deleted() == y.is_deleted()`).
6. Both blocks must hold content of the same type.
7. The block content type itself must support merging. There's are only several types which support that operation:
    - `ContentType::Atom`
    - `ContentType::Json`
    - `ContentType::String`
    - `ContentType::Deleted`

The merge operation itself works as follows: block `y` which is merged into block `x` is adding its own clock length to
`x`'s and overriding `x.right` neighbor with its own `y.right` neighbor. Additionally, we can try to merge the content (
user data) together. However, this depends on the content type and the fact if content data itself can be inlined within
the `BlockHeader`:

- `ContentType::Atom` and `ContentType::Json` are never merged. Moreover, if they did contain an inlined content, that
  content is extracted and stored as a separate entries (one per element) in a `ContentStore`.
- `ContentType::Deleted` doesn't have associated user data, so no action is necessary.
- `ContentType::String` uses string concatenation. Contents of both blocks are concatenated together and saved back. If
  the result of concatenation is longer than max capacity allowed by the `BlockHeader`'s inlined content, that content
  will be moved to `ContentStore`.

## Block splitting

Inversely to block merging, block splitting is an operation which splits a single block representing a sequence of
inserted elements into two. The reason for that is that if we need to insert a new element (wrapped in a block) in
between two other existing elements, that happened to be wrapped together within the single block element. Such block
needs to be split in two, so that resulting blocks can correctly keep the information about newly inserted block as
their `right` and `left` neighbor respectively.

Splitting block `x` at a given `offset` - assuming that `offset` exists within the boundaries of `x` itself (
`offset > 0 && offset < x.clock_len()`) - means producing a new block `y` and updating `x` itself in a way such as:

- New block `y` gets an `ID` with the same client as `x.id` and clock equal to `x.id.clock + offset`.
- Block's `y` `left` neighbor and `origin_left` is set to the last element of `x`.
- Block's `x` right neighbor is set to `y.id`.
- Block `y` clock length is set to `x.clock_len() - offset`, and then `x` clock length is set to `offset`.
- Other block metadata for `y` is copied from `x`.

The final step is about splitting the content itself if necessary. As discussed above, the only type in `ysr` library
that is subject of concatenation is `ContentType::String`. Therefore, splitting the string only happens for that type.
Since (for compatiblity with original `yjs` library) clock length for strings is counted as number of UTF-16 codepoints,
the string split index needs to be computed with respect to UTF-16 codepoints first. Once string is split in two, we can
try to insert two string fragments as inlined contents of their respective blocks, if they are small enough to fit in.
If the original string content belonging to `x` resided in `ContentStore`, but could be fit into inlined content after
splitting, the original content entry should be removed from the `ContentStore`.

## YATA for maps

While *YATA* conflict resolution algorithm targets conflict resolution in inserting data into indexed sequences of
elements, it can be extended to work with the maps. In such case each map entry could be treated as its own individual
sequence of elements.

Reading from the map means reading the last element of that map entry's sequence of blocks. This is executed by lookup
on `MapEntryStore` for a particular (`NodeId`, `{search_key}`) key-value pair, reading the value holding an `ID` of the
block holding the corresponding entry value and then resolving that value. Keep in mind that the block itself might have
been deleted.

Writing (or integrating the value) to the map means, deleting last known element in the sequence and then appending the
new element block as the next last element.

In `ysr` the entries of the individual maps are kept in the `MapEntryStore`. Since - in order to read - we usually
require the last element in the sequence, value stored in `MapEntryStore` is the `Id` of the last block in the sequence.