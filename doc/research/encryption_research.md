# Ciphertext research

Useful streaming encrypted stuff:
* [encrypted-stream](https://github.com/nknorg/encrypted-stream) does a number of streaming cipher suites over a `net.Conn`

https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation


# Maximum number of blocks

It's important to never use more than 2^32 random nonces with a given key because of the risk of a repeat. For this reason, we shouldn't ever encrypt a DAG that's larger than ~4.2M blocks or 7.62939453125PiB. So... we don't support encrypted 7PiB files. I think that'll be ok.

## Block Cipher Mode of Operation

Streaming encryption comes in two "flavours":
* Galois/Counter Mode (GCM)
* Cipher block chaining (CBC)
* Cipher feedback (CFB)

### GCM

| Galois/counter (GCM)      |     |
|---------------------------|-----|
| Encryption parallelizable | Yes |
| Decryption parallelizable | Yes |
| Random read access        | Yes |


### Cipher block chaining

| Cipher block chaining (CBC) |     |
|-----------------------------|-----|
| Encryption parallelizable   | No  |
| Decryption parallelizable   | Yes |
| Random read access          | Yes |

CBC has been the most commonly used mode of operation. Its main drawbacks are that encryption is sequential (i.e., it cannot be parallelized), and that the message must be padded to a multiple of the cipher block size. One way to handle this last issue is through the method known as ciphertext stealing. **Note that a one-bit change in a plaintext or initialization vector (IV) affects all following ciphertext blocks.**

### Cipher Feedback Mode

| Cipher feedback (CFB)     |     |
|---------------------------|-----|
| Encryption parallelizable | No  |
| Decryption parallelizable | Yes |
| Random read access        | Yes |

The cipher feedback (CFB) mode, in its simplest form is using the entire output of the block cipher. In this variation, it is very similar to CBC, makes a block cipher into a self-synchronizing stream cipher. CFB decryption in this variation is almost identical to CBC encryption performed in reverse

Like CBC mode, changes in the plaintext propagate forever in the ciphertext, and encryption cannot be parallelized. Also like CBC, decryption can be parallelized.

## Implementation

After a first pass of switching to a "streaming" format, numbers don't look so hot:

```
$ benchcmp ./doc/a.txt ./doc/b.txt
benchmark                                                old ns/op     new ns/op     delta
BenchmarkIPFSCat10MbFile-4                               29719         27894         -6.14%
BenchmarkIPFSWrite10MbFile-4                             59490         39404         -33.76%
BenchmarkIPFSCat10MbFileSubdir-4                         43188         34251         -20.69%
BenchmarkIPFSWrite10MbFileSubdir-4                       85165         79600         -6.53%
BenchmarkIPFSCp10DirectoriesWithOne10MbFileEach-4        4256106       1749986       -58.88%
BenchmarkPublicCat10MbFile-4                             53312         30936         -41.97%
BenchmarkPublicWrite10MbFile-4                           302985        237243        -21.70%
BenchmarkPublicCat10MbFileSubdir-4                       65502         52222         -20.27%
BenchmarkPublicWrite10MbFileSubdir-4                     446950        465322        +4.11%
BenchmarkPublicCp10DirectoriesWithOne10MbFileEach-4      5100          4223          -17.20%
BenchmarkPrivateCat10MbFile-4                            59309         116868        +97.05%
BenchmarkPrivateWrite10MbFile-4                          526355        764233        +45.19%
BenchmarkPrivateCat10MbFileSubdir-4                      85197         248621        +191.82%
BenchmarkPrivateWrite10MbFileSubdir-4                    991575        1416363       +42.84%
BenchmarkPrivateCp10DirectoriesWithOne10MbFileEach-4     39234         14033991      +35669.97%

benchmark                                                old allocs     new allocs     delta
BenchmarkIPFSCat10MbFile-4                               42             42             +0.00%
BenchmarkIPFSWrite10MbFile-4                             204            204            +0.00%
BenchmarkIPFSCat10MbFileSubdir-4                         123            123            +0.00%
BenchmarkIPFSWrite10MbFileSubdir-4                       524            524            +0.00%
BenchmarkIPFSCp10DirectoriesWithOne10MbFileEach-4        2473           2472           -0.04%
BenchmarkPublicCat10MbFile-4                             108            108            +0.00%
BenchmarkPublicWrite10MbFile-4                           1168           1168           +0.00%
BenchmarkPublicCat10MbFileSubdir-4                       243            243            +0.00%
BenchmarkPublicWrite10MbFileSubdir-4                     2029           2029           +0.00%
BenchmarkPublicCp10DirectoriesWithOne10MbFileEach-4      21             19             -9.52%
BenchmarkPrivateCat10MbFile-4                            119            114            -4.20%
BenchmarkPrivateWrite10MbFile-4                          2304           2260           -1.91%
BenchmarkPrivateCat10MbFileSubdir-4                      187            185            -1.07%
BenchmarkPrivateWrite10MbFileSubdir-4                    3175           3153           -0.69%
BenchmarkPrivateCp10DirectoriesWithOne10MbFileEach-4     119            32847          +27502.52%

benchmark                                                old bytes     new bytes     delta
BenchmarkIPFSCat10MbFile-4                               49794         49800         +0.01%
BenchmarkIPFSWrite10MbFile-4                             11346         11420         +0.65%
BenchmarkIPFSCat10MbFileSubdir-4                         53398         53422         +0.04%
BenchmarkIPFSWrite10MbFileSubdir-4                       27172         27075         -0.36%
BenchmarkIPFSCp10DirectoriesWithOne10MbFileEach-4        252153        253425        +0.50%
BenchmarkPublicCat10MbFile-4                             53608         53599         -0.02%
BenchmarkPublicWrite10MbFile-4                           67176         67056         -0.18%
BenchmarkPublicCat10MbFileSubdir-4                       61998         61996         -0.00%
BenchmarkPublicWrite10MbFileSubdir-4                     116272        117456        +1.02%
BenchmarkPublicCp10DirectoriesWithOne10MbFileEach-4      1272          1114          -12.42%
BenchmarkPrivateCat10MbFile-4                            115482        335819        +190.80%
BenchmarkPrivateWrite10MbFile-4                          156308        1270808       +713.02%
BenchmarkPrivateCat10MbFileSubdir-4                      123369        482776        +291.33%
BenchmarkPrivateWrite10MbFileSubdir-4                    218098        1752094       +703.35%
BenchmarkPrivateCp10DirectoriesWithOne10MbFileEach-4     12854         18209168      +141561.49%
```

I think the next approach should fuse a fork of the chunker with an encryption format


Next to research:
* Need to take a look at ceramic's dag-cose
  * nope, base64 encoding at the file layer isn't going to be performant enough
* Look into car format varint prefixes

