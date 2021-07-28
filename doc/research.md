# research for a wnfs v2

# Performance

## Switching to `dag-cbor` headers, inlining metadata

Doing a _very_ naive experiment to see what switching to `dag-cbor` headers would look like. Switching to `dag-cbor` affords us the chance to pack arbitrary data in the header block, saving one block per file & tree.

In this instance I'm using the "old" [go-ipld-cbor](http://github.com/ipfs/go-ipld-cbor) package because it's what presently works with go-ipfs.

comparing just switching `dag-pb` format to `dag-cbor` headers:
```
$ benchcmp bench_dag_pb_headers.txt bench_cbor_headers.txt
benchmark                                               old ns/op     new ns/op     delta
BenchmarkPublicCat10MbFile-4                            33671         48160         +43.03%
BenchmarkPublicWrite10MbFile-4                          264474        305450        +15.49%
BenchmarkPublicCat10MbFileSubdir-4                      52110         70329         +34.96%
BenchmarkPublicWrite10MbFileSubdir-4                    426310        530709        +24.49%
BenchmarkPublicCp10DirectoriesWithOne10MbFileEach-4     2474          3982          +60.95%
```

`dag-cbor` is slower across the board, which isn't surprising. protobuf isn't as flexible as cbor, but the advantage should come in the form of better serialization speeds. But with `dag-cbor` we can inline the `metadata` component, saving a block on each file & tree.

comparing `dag-pb` to `dag-cbor` headers, this time with metadata recorded directly in the block:
```
$ benchcmp bench_dag_pb_headers.txt bench_cbor_headers_inline_metadata.txt
benchmark                                               old ns/op     new ns/op     delta
BenchmarkPublicCat10MbFile-4                            33671         40457         +20.15%
BenchmarkPublicWrite10MbFile-4                          264474        251295        -4.98%
BenchmarkPublicCat10MbFileSubdir-4                      52110         71086         +36.42%
BenchmarkPublicWrite10MbFileSubdir-4                    426310        452925        +6.24%
BenchmarkPublicCp10DirectoriesWithOne10MbFileEach-4     2474          3000          +21.26%
```
