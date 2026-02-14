# StateVector

`StateVector` structure behavior in `ysr` adheres to a [Vector Clock](https://en.wikipedia.org/wiki/Vector_clock)
definition. Essentially, it's a map of `ClientID`->`Clock` key-value pairs representing all integrated insertions within
the scope of corresponding document. It's also a part of [IDSet](./id_sets.md).

```rust
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StateVector(BTreeMap<ClientID, Clock>);
```

`StateVector` offers a capability of checking if a given `ID` can be found within the current state vector: a `contains`
method returns true if an `ID` (which represents a `ClientID`->`Clock` key-value pair) has a `Clock` lower than or
equal to corresponding entry in current `StateVector` (identified by the same `ClientID`).
