# Intern Strings Store

An `InternStringsStore` is a dedicated storage used for storing interned strings, that can be reused in-between various
`ysr` components, such as:

- Root-level nodes, which are referred to using their user-defined string names rather than block IDs like nested nodes
  do.

`InternStringsStore` is designed to only store a handful of elements.

When inserting a new string, we generate a new consistent hash using `twox_hash::XxHash32` algorithm, and then we store
a `Hash`->`String` key-value pair inside of intern strings store, where `Hash` is big-endian encoded 32-bit unsigned
integer. DISCLAIMER: *since we operate on rather small hashes it's important to only use a small number of strings
here, another is that, once inserted intern strings are never deleted*. Whenever we try to insert a new value under
given hash, and it turns out that there existed a different value under the same hash, and error should be returned.