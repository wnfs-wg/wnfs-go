# Fission Webnative Filesystem: Implementation Notes


## Posix Implementation details

* `mtime` & `ctime` datestamps are in millisecond precision


## Serialization formats:

Data stored in the header layer of IPFS is stored as raw DAG nodes in various 
formats:

| section   | raw data format | notes |
| --------- | ----------- | ----- |
| header    | CBOR | |
| skeleton  | CSV | |
| semver    | text | |

# IPFS

* MUST use v1 CIDS
* IPLD DAG Nodes only contain links

### required core methods
```

```