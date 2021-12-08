module github.com/qri-io/wnfs-go

go 1.16

replace github.com/ipfs/go-ipld-cbor => ./go-ipld-cbor

require (
	github.com/dustin/go-humanize v1.0.0
	github.com/filecoin-project/go-hamt-ipld/v3 v3.1.1-0.20210921153832-8cf7cf9309c8
	github.com/fxamacker/cbor/v2 v2.3.0
	github.com/google/go-cmp v0.5.5
	github.com/google/gofuzz v1.2.0
	github.com/gopherjs/gopherjs v0.0.0-20190812055157-5d271430af9f // indirect
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-blockservice v0.2.1
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-cidutil v0.0.2
	github.com/ipfs/go-ds-flatfs v0.5.1
	github.com/ipfs/go-ipfs-blockstore v1.1.1
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-files v0.0.8
	github.com/ipfs/go-ipld-cbor v0.0.5
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-log v1.0.5
	github.com/ipfs/go-merkledag v0.5.1
	github.com/ipfs/go-unixfs v0.2.5
	github.com/labstack/echo/v4 v4.6.1
	github.com/libp2p/go-buffer-pool v0.0.2
	github.com/libp2p/go-libp2p-record v0.1.3 // indirect
	github.com/multiformats/go-multihash v0.0.15
	github.com/pierrec/xxHash v0.1.5
	github.com/sergi/go-diff v1.2.0
	github.com/smartystreets/assertions v1.0.1 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
	github.com/whyrusleeping/cbor-gen v0.0.0-20210713220151-be142a5ae1a8
	github.com/xlab/treeprint v1.1.0
	golang.org/x/crypto v0.0.0-20210817164053-32db794688a5
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
	golang.org/x/net v0.0.0-20210913180222-943fd674d43e
	golang.org/x/tools v0.1.1 // indirect
)
