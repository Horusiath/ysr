# Conflict-free Replicated Data Types

This document explains the concept of *Conflict-free Replicated Data Types* (CRDT for short) in context of `ysr`
library.

`ysr` could be categorized as a delta-state CRDT. Each document represents its state as a collection
of [blocks](../blocks.md).

Each block represents one or more elements inserted into document within the same `Node` scope. Every inserted element
receives an [ID](../id.md), which allows us to uniquely identify that insertion operation within the scope of its parent
document. The most recent `ID`s for each client together build something called [StateVector](../state_vector.md), which
serves as a causal timestamp used for efficient reporting about the sync state of the document. Different client can
determine each other's sync status by sending each other their own `StateVectors` and then calculating the delta
updates - this can be done using `Transaction::diff_update`. These updates can be then send over the network to remote
clients to bring them up to date about the missing changes known to a local client.

`StateVector`s alone can only convey the status of inserted elements, but won't be enough to inform about elements that
have been already known to the remote clients, but have been deleted since then. For that we use
an [IDSet](../id_sets.md): a structure which provides a compact representation of ranges of block IDs, in context of
updates used to determine ranges of deleted block data. `IDSet` is not send to remote clients, instead when computing
the delta update, an `IDSet` of the entire document (representing all deleted elements in the document's history) is
also encoded as part of the update data.

Since sending the `IDSet` of an entire document with each change is not efficient, updates can also be produced
incrementally when the read-write `Transaction` is committed, as part of `TransactionSummary` passed to
`Transaction::commit`. These updates contain only changes captured as part of the individual `Transaction`, and their
`IDSet`s are representing only elements deleted in the scope of current `Transaction`. Besides that, the binary format
of both updates is exactly the same.

As such updates satisfy 3 core properties of state-based CRDT:

1. **Idempotency**: an update can be applied to a document any number of times. All inserted and deleted elements will
   be deduplicated based on their `ID`s.
2. **Associativity**: individual updates can be combined into greater updates representing bigger set of changes.
   These updates can be further applied in full or partially to the document to produce the same results as if updates
   building their respective parts were to be applied separately.
3. **Commutativity**: an order in which updates are applied, doesn't matter. While some update blocks can have a
   dependency relationships between one another, and as such cannot be integrated until all of their dependencies are
   integrated into the document first, they will be simply stashed aside, waiting until their dependencies are satisfied
   and integrated afterwards.

For as long as `ClientID` is unique among all clients actively inserting new elements to their document replicas, it's
possible for the multiple clients to collaborate over the same data collections (`Nodes`). While it's possible that
inserts will generate a conflicts to occur, these conflicts will be resolved accordingly to [YATA algorithm](./yata.md).

As for deletions: since `ysr` only recognizes two types of operations (inserts and deletions), once inserted element is
removed can no longer be reached or modified again. If necessary another element with the same value can be
inserted, but from the *YATA* algorithm perspective this counts as a new insertion with new `ID` being generated. 