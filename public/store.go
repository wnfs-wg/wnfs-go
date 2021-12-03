package public

import (
	"context"
	"fmt"
	"io"
	"io/fs"

	blockservice "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	cidutil "github.com/ipfs/go-cidutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	cbornode "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	merkledag "github.com/ipfs/go-merkledag"
	balanced "github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	unixfsio "github.com/ipfs/go-unixfs/io"
	multihash "github.com/multiformats/go-multihash"
	base "github.com/qri-io/wnfs-go/base"
)

// Store is a store of Content-Addressed block data indexed by merkle
// proofs. It's an abstraction over IPFS that defines the required feature set
// to run wnfs
type Store interface {
	Context() context.Context
	Blockservice() blockservice.BlockService
	GetFile(ctx context.Context, root cid.Cid) (io.ReadCloser, error)
	PutFile(f fs.File) (PutResult, error)
}

func NodeStore(n base.Node) (Store, error) {
	st, err := n.Stat()
	if err != nil {
		return nil, err
	}
	store, ok := st.Sys().(Store)
	if !ok {
		return nil, fmt.Errorf("node Sys is not a Store")
	}
	return store, nil
}

type store struct {
	ctx     context.Context
	bserv   blockservice.BlockService
	dagserv format.DAGService
}

var _ Store = (*store)(nil)

func NewStore(ctx context.Context, bserv blockservice.BlockService) Store {
	return &store{
		ctx:     ctx,
		bserv:   bserv,
		dagserv: merkledag.NewDAGService(bserv),
	}
}

func (mds *store) Context() context.Context                { return mds.ctx }
func (mds *store) Blockservice() blockservice.BlockService { return mds.bserv }

func (mds *store) PutFile(f fs.File) (PutResult, error) {
	// dserv := format.NewBufferedDAG(mds.ctx, mds.dagserv)
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return PutResult{}, err
	}
	prefix.MhType = multihash.SHA2_256

	spl := chunker.NewSizeSplitter(f, 1024*256)
	dbp := ihelper.DagBuilderParams{
		Maxlinks: 1024,

		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   32,
		},

		Dagserv: mds.dagserv,
	}

	db, err := dbp.New(spl)
	if err != nil {
		return PutResult{}, err
	}

	root, err := balanced.Layout(db)
	if err != nil {
		return PutResult{}, err
	}

	rootSize, err := root.Size()
	if err != nil {
		return PutResult{}, err
	}

	return PutResult{
		Cid:  root.Cid(),
		Size: int64(rootSize),
	}, nil
}

func (mds *store) GetFile(ctx context.Context, root cid.Cid) (io.ReadCloser, error) {
	ses := merkledag.NewSession(ctx, mds.dagserv)

	nd, err := ses.Get(ctx, root)
	if err != nil {
		return nil, err
	}

	return unixfsio.NewDagReader(ctx, nd, ses)
}

// Copy blocks from src to dst
func CopyBlocks(ctx context.Context, id cid.Cid, src, dst Store) error {
	blk, err := src.Blockservice().GetBlock(ctx, id)
	if err != nil {
		return err
	}

	if blk.Cid().Type() == cid.DagCBOR {
		n, err := cbornode.DecodeBlock(blk)
		if err != nil {
			return err
		}
		log.Debugw("CopyBlock", "cid", n.Cid(), "len(links)", len(n.Links()))
		for _, l := range n.Links() {
			if err := CopyBlocks(ctx, l.Cid, src, dst); err != nil {
				return fmt.Errorf("copying block %q: %w", l.Cid, err)
			}
		}
	}

	return dst.Blockservice().Blockstore().Put(blk)
}

func AllKeys(ctx context.Context, bs blockstore.Blockstore) ([]cid.Cid, error) {
	keys, err := bs.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}

	ids := make([]cid.Cid, 0)
	for id := range keys {
		ids = append(ids, id)
	}

	return ids, nil
}

type PutResult struct {
	Cid      cid.Cid
	Size     int64
	Type     base.NodeType
	Userland cid.Cid
	Metadata cid.Cid
	Skeleton Skeleton
}

var _ base.PutResult = (*PutResult)(nil)

func (r PutResult) CID() cid.Cid {
	return r.Cid
}

func (r PutResult) ToLink(name string) base.Link {
	return base.Link{
		Name:   name,
		Cid:    r.Cid,
		Size:   r.Size,
		IsFile: (r.Type == base.NTFile || r.Type == base.NTDataFile),
	}
}

func (r PutResult) ToSkeletonInfo() SkeletonInfo {
	return SkeletonInfo{
		Cid:         r.Cid,
		Metadata:    r.Metadata,
		Userland:    r.Userland,
		SubSkeleton: r.Skeleton,
		IsFile:      (r.Type == base.NTFile || r.Type == base.NTDataFile),
	}
}
