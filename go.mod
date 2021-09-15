module github.com/qri-io/wnfs-go

go 1.16

// adds a node.Write(ctx) method. See: https://github.com/filecoin-project/go-hamt-ipld/pull/94
replace github.com/filecoin-project/go-hamt-ipld/v3 => github.com/qri-io/go-hamt-ipld/v3 v3.1.1-0.20210829174419-d5dd13402d27

require (
	github.com/bits-and-blooms/bloom/v3 v3.0.1
	github.com/filecoin-project/go-hamt-ipld/v3 v3.1.0
	github.com/fxamacker/cbor/v2 v2.3.0
	github.com/google/go-cmp v0.5.5
	github.com/google/uuid v1.2.0 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20190812055157-5d271430af9f // indirect
	github.com/ipfs/go-bitswap v0.3.4 // indirect
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-blockservice v0.1.4
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-cidutil v0.0.2
	github.com/ipfs/go-ds-flatfs v0.4.5
	github.com/ipfs/go-ipfs-blockstore v1.0.5-0.20210802214209-c56038684c45
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-files v0.0.8
	github.com/ipfs/go-ipfs-pinner v0.1.1
	github.com/ipfs/go-ipfs-posinfo v0.0.1
	github.com/ipfs/go-ipld-cbor v0.0.5
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-log v1.0.5
	github.com/ipfs/go-merkledag v0.3.2
	github.com/ipfs/go-mfs v0.1.2
	github.com/ipfs/go-path v0.0.9 // indirect
	github.com/ipfs/go-unixfs v0.2.5
	github.com/libp2p/go-buffer-pool v0.0.2
	github.com/libp2p/go-libp2p v0.14.2 // indirect
	github.com/libp2p/go-libp2p-record v0.1.3 // indirect
	github.com/multiformats/go-multiaddr v0.3.2 // indirect
	github.com/multiformats/go-multihash v0.0.15
	github.com/polydawn/refmt v0.0.0-20201211092308-30ac6d18308e // indirect
	github.com/smartystreets/assertions v1.0.1 // indirect
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/warpfork/go-wish v0.0.0-20200122115046-b9ea61034e4a // indirect
	github.com/whyrusleeping/cbor-gen v0.0.0-20210713220151-be142a5ae1a8
	golang.org/x/crypto v0.0.0-20210506145944-38f3c27a63bf // indirect
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
	golang.org/x/sys v0.0.0-20210511113859-b0526f3d8744 // indirect
	golang.org/x/tools v0.1.1 // indirect
)
