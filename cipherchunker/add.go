package cipherchunker

import (
	"context"
	"crypto/cipher"
	"io"
	gopath "path"

	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	pin "github.com/ipfs/go-ipfs-pinner"
	posinfo "github.com/ipfs/go-ipfs-posinfo"
	ipld "github.com/ipfs/go-ipld-format"
	golog "github.com/ipfs/go-log"
	mfs "github.com/ipfs/go-mfs"
	unixfs "github.com/ipfs/go-unixfs"
	balanced "github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	trickle "github.com/ipfs/go-unixfs/importer/trickle"
	multihash "github.com/multiformats/go-multihash"
)

var log = golog.Logger("cryptchunker")

type Adder struct {
	ctx        context.Context
	bufferedDS *ipld.BufferedDAG
	dagService ipld.DAGService
	pinning    pin.Pinner
	gcLocker   bstore.GCLocker
	CidBuilder cid.Builder
	mroot      *mfs.Root
	cipher     cipher.AEAD
	unlocker   bstore.Unlocker
	tempRoot   cid.Cid
	Trickle    bool
	Pin        bool
}

func NewAdder(ctx context.Context, bs bstore.GCLocker, pinning pin.Pinner, ds ipld.DAGService, auth cipher.AEAD) (*Adder, error) {
	bufferedDS := ipld.NewBufferedDAG(ctx, ds)

	return &Adder{
		ctx:        ctx,
		bufferedDS: bufferedDS,
		dagService: ds,
		gcLocker:   bs,
		cipher:     auth,
		Pin:        true,
		pinning:    pinning,
		CidBuilder: cid.V1Builder{
			Codec:    cid.DagProtobuf,
			MhType:   multihash.SHA2_256,
			MhLength: -1,
		},
	}, nil
}

func (adder *Adder) mfsRoot() (*mfs.Root, error) {
	if adder.mroot != nil {
		return adder.mroot, nil
	}
	rnode := unixfs.EmptyDirNode()
	rnode.SetCidBuilder(adder.CidBuilder)
	mr, err := mfs.NewRoot(adder.ctx, adder.dagService, rnode, nil)
	if err != nil {
		return nil, err
	}
	adder.mroot = mr
	return adder.mroot, nil
}

// SetMfsRoot sets `r` as the root for Adder.
func (adder *Adder) SetMfsRoot(r *mfs.Root) {
	adder.mroot = r
}

func (adder *Adder) add(reader io.Reader) (ipld.Node, error) {
	// chnk, err := chunker.FromString(reader, chunkerType)
	chnk, err := NewCipherSplitter(reader, adder.cipher, uint32(chunker.DefaultBlockSize))
	if err != nil {
		return nil, err
	}

	params := ihelper.DagBuilderParams{
		Dagserv: adder.bufferedDS,
		// TODO(b5): what's the advantage of raw leaves?
		RawLeaves: true,
		Maxlinks:  ihelper.DefaultLinksPerBlock,
		NoCopy:    false,
		CidBuilder: cid.V1Builder{
			MhType:   multihash.SHA2_256,
			MhLength: -1,
		},
	}

	db, err := params.New(chnk)
	if err != nil {
		return nil, err
	}
	var nd ipld.Node
	if adder.Trickle {
		nd, err = trickle.Layout(db)
	} else {
		nd, err = balanced.Layout(db)
	}
	if err != nil {
		return nil, err
	}

	return nd, adder.bufferedDS.Commit()
}

func (adder *Adder) addNode(node ipld.Node, path string) error {
	// patch it into the root
	if path == "" {
		path = node.Cid().String()
	}

	if pi, ok := node.(*posinfo.FilestoreNode); ok {
		node = pi.Node
	}

	mr, err := adder.mfsRoot()
	if err != nil {
		return err
	}
	dir := gopath.Dir(path)
	if dir != "." {
		opts := mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: adder.CidBuilder,
		}
		if err := mfs.Mkdir(mr, dir, opts); err != nil {
			return err
		}
	}

	if err := mfs.PutNode(mr, path, node); err != nil {
		return err
	}

	// if !adder.Silent {
	// 	return outputDagnode(adder.Out, path, node)
	// }
	return nil
}

// Recursively pins the root node of Adder and
// writes the pin state to the backing datastore.
func (adder *Adder) PinRoot(root ipld.Node) error {
	if !adder.Pin {
		return nil
	}

	rnk := root.Cid()

	err := adder.dagService.Add(adder.ctx, root)
	if err != nil {
		return err
	}

	if adder.tempRoot.Defined() {
		err := adder.pinning.Unpin(adder.ctx, adder.tempRoot, true)
		if err != nil {
			return err
		}
		adder.tempRoot = rnk
	}

	adder.pinning.PinWithMode(rnk, pin.Recursive)
	return adder.pinning.Flush(adder.ctx)
}

func (adder *Adder) AddFileAndPin(reader io.Reader) (ipld.Node, error) {
	if adder.Pin {
		adder.unlocker = adder.gcLocker.PinLock()
	}
	defer func() {
		if adder.unlocker != nil {
			adder.unlocker.Unlock()
		}
	}()

	nd, err := adder.add(reader)
	if err != nil {
		return nil, err
	}
	return nd, adder.PinRoot(nd)
}
