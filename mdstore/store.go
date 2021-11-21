package mdstore

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"sort"

	blocks "github.com/ipfs/go-block-format"
	blockservice "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	cidutil "github.com/ipfs/go-cidutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	cbornode "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	golog "github.com/ipfs/go-log"
	merkledag "github.com/ipfs/go-merkledag"
	balanced "github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	unixfsio "github.com/ipfs/go-unixfs/io"
	multihash "github.com/multiformats/go-multihash"
)

var log = golog.Logger("wnfs")

// MerkleDagStore is a store of Content-Addressed block data indexed by merkle
// proofs. It's an abstraction over IPFS that defines the required feature set
// to run wnfs
type MerkleDagStore interface {
	Blockservice() blockservice.BlockService

	GetFile(ctx context.Context, root cid.Cid) (io.ReadCloser, error)
	PutFile(f fs.File) (PutResult, error)
}

type merkleDagStore struct {
	ctx     context.Context
	bserv   blockservice.BlockService
	dagserv format.DAGService
}

var _ MerkleDagStore = (*merkleDagStore)(nil)

func NewMerkleDagStore(ctx context.Context, bserv blockservice.BlockService) (MerkleDagStore, error) {
	if bserv == nil {
		return nil, fmt.Errorf("blockservice cannot be nil")
	}

	return &merkleDagStore{
		ctx:     ctx,
		bserv:   bserv,
		dagserv: merkledag.NewDAGService(bserv),
	}, nil
}

func (mds *merkleDagStore) Blockservice() blockservice.BlockService { return mds.bserv }

func (mds *merkleDagStore) PutFile(f fs.File) (PutResult, error) {
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

func (mds *merkleDagStore) GetFile(ctx context.Context, root cid.Cid) (io.ReadCloser, error) {
	ses := merkledag.NewSession(ctx, mds.dagserv)

	nd, err := ses.Get(ctx, root)
	if err != nil {
		return nil, err
	}

	return unixfsio.NewDagReader(ctx, nd, ses)
}

// Copy blocks from src to dst
func CopyBlocks(ctx context.Context, id cid.Cid, src, dst MerkleDagStore) error {
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

type Links struct {
	l map[string]Link
}

func NewLinks(links ...Link) Links {
	lks := Links{
		l: map[string]Link{},
	}

	for _, link := range links {
		lks.Add(link)
	}

	return lks
}

func DecodeLinksBlock(blk blocks.Block) (Links, error) {
	lks := Links{l: map[string]Link{}}
	n, err := cbornode.DecodeBlock(blk)
	if err != nil {
		return lks, err
	}
	for _, l := range n.Links() {
		lks.l[l.Name] = Link{
			Name: l.Name,
			Cid:  l.Cid,
			Size: int64(l.Size),
		}
	}
	return lks, nil
}

func (ls Links) Len() int {
	return len(ls.l)
}

func (ls Links) Add(link Link) {
	ls.l[link.Name] = link
}

func (ls Links) Remove(name string) bool {
	_, existed := ls.l[name]
	delete(ls.l, name)
	return existed
}

func (ls Links) Get(name string) *Link {
	lk, ok := ls.l[name]
	if !ok {
		return nil
	}

	return &lk
}

func (ls Links) Slice() []Link {
	links := make([]Link, 0, len(ls.l))
	for _, link := range ls.l {
		links = append(links, link)
	}
	return links
}

func (ls Links) SortedSlice() []Link {
	names := make([]string, 0, len(ls.l))
	for name := range ls.l {
		names = append(names, name)
	}
	sort.Strings(names)

	links := make([]Link, 0, len(ls.l))
	for _, name := range names {
		links = append(links, ls.l[name])
	}
	return links
}

func (ls Links) Map() map[string]Link {
	return ls.l
}

func (ls Links) EncodeBlock() (blocks.Block, error) {
	links := map[string]cid.Cid{}
	for k, l := range ls.l {
		links[k] = l.Cid
	}
	return cbornode.WrapObject(links, multihash.SHA2_256, -1)
}

type Link struct {
	Name string
	Size int64
	Cid  cid.Cid

	IsFile bool
	Mtime  int64
}

func (l Link) IsEmpty() bool {
	return l.Cid.String() == ""
}

func (l Link) IPLD() *format.Link {
	return &format.Link{
		Name: l.Name,
		Cid:  l.Cid,
		Size: uint64(l.Size),
	}
}

type PutResult struct {
	Cid  cid.Cid
	Size int64
}

func (pr *PutResult) ToLink(name string, isFile bool) Link {
	return Link{
		Name:   name,
		IsFile: isFile,

		Cid:  pr.Cid,
		Size: pr.Size,
	}
}
