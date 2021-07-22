# Implementation Notes & Questions

* Embedding `size` and `isFile` values in link objects - would require IPLD CBOR

### Lots of little files scenario
Possible to create a special case where data is inlined into header block if file will fit?

### Skipping UnixFS dirs entirely?
Seems like a low priority, would need to chat with PL folks