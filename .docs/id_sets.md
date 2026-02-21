# ID Set

`IDSet` is a structure that allows to mark an entire ranges of elements existing within a document. The most notorious
use case for this is **delete set** which is used to mark which elements and blocks within the document has been deleted
and is included as part of the document update data.

`IDSet` structure is essentially a map of list of ranges, where each key-value entry could be defined as `ClientID`->
`[(Clock,Clock)]`. The list of ranges describes IDs of elements within the blocks created by specific `ClientID`. Each
range containing `(Clock, Clock)` pair defines a start and end clock id of the range. Therefore, `IDSet` could be used
as a `BlockRange` iterator.

`IDSet` exposes some of the common set operations such as:

- `merge` which is used i.e. when two updates containing delete sets are merged together. In such case both `IDSet` maps
  merge their corresponding entries. If both of them shared the same entry, the `(Clock, Clock)` ranges describing start
  and end positions are merged with start-end ranges of another set. If those sets overlap (
  `!(a.end < b.start || a.start > b.end)`), they can be squashed together (`(a.start.min(b.start), a.end.max(b.end))`).
- `contains` which determines if a given `ID` can be found within a current `IDSet`.