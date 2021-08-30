module github.com/qri-io/wnfs-go

go 1.16

// adds a node.Write(ctx) method. See: https://github.com/filecoin-project/go-hamt-ipld/pull/94
replace github.com/filecoin-project/go-hamt-ipld/v3 => github.com/qri-io/go-hamt-ipld/v3 v3.1.1-0.20210829174419-d5dd13402d27

require (
	github.com/bits-and-blooms/bloom/v3 v3.0.1
	github.com/dustin/go-humanize v1.0.0
	github.com/filecoin-project/go-hamt-ipld/v3 v3.1.0 // indirect
	github.com/fxamacker/cbor/v2 v2.3.0
	github.com/google/go-cmp v0.5.5
	github.com/ipfs/go-blockservice v0.1.4
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-filestore v0.0.3
	github.com/ipfs/go-ipfs v0.9.0
	github.com/ipfs/go-ipfs-blockstore v0.1.6
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-config v0.14.0
	github.com/ipfs/go-ipfs-files v0.0.8
	github.com/ipfs/go-ipfs-pinner v0.1.1
	github.com/ipfs/go-ipfs-posinfo v0.0.1
	github.com/ipfs/go-ipld-cbor v0.0.5 // indirect
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-log v1.0.5
	github.com/ipfs/go-merkledag v0.3.2
	github.com/ipfs/go-mfs v0.1.2
	github.com/ipfs/go-unixfs v0.2.5
	github.com/ipfs/interface-go-ipfs-core v0.4.0
	github.com/libp2p/go-buffer-pool v0.0.2
	github.com/libp2p/go-libp2p v0.14.2
	github.com/libp2p/go-libp2p-core v0.8.5
	github.com/libp2p/go-libp2p-peerstore v0.2.7
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.4.1
	github.com/multiformats/go-multihash v0.0.15
	github.com/qri-io/qfs v0.6.0
	github.com/urfave/cli/v2 v2.3.0
	github.com/whyrusleeping/cbor-gen v0.0.0-20210713220151-be142a5ae1a8 // indirect
	github.com/xlab/treeprint v1.1.0
)
