package mdstore

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"sort"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	chunker "github.com/ipfs/go-ipfs-chunker"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	unixfsio "github.com/ipfs/go-unixfs/io"
	"github.com/multiformats/go-multihash"
)

// MerkleDagStore is a store of Content-Addressed block data indexed by merkle
// proofs. It's an abstraction over IPFS that defines the required feature set
// to run wnfs
type MerkleDagStore interface {
	Blockservice() blockservice.BlockService

	GetBlock(ctx context.Context, id cid.Cid) (d []byte, err error)
	PutBlock(d []byte) (id cid.Cid, err error)

	GetNode(ctx context.Context, id cid.Cid) (DagNode, error)
	PutNode(links Links) (PutResult, error)

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

func (mds *merkleDagStore) GetNode(ctx context.Context, id cid.Cid) (DagNode, error) {
	node, err := mds.dagserv.Get(mds.ctx, id)
	if err != nil {
		return nil, err
	}

	size, err := node.Size()
	if err != nil {
		return nil, err
	}

	return &dagNode{
		id:   id,
		size: int64(size),
		node: node,
	}, nil
}

func (mds *merkleDagStore) PutNode(links Links) (PutResult, error) {
	node := unixfs.EmptyDirNode()
	// node := &merkledag.ProtoNode{}
	// node.SetData(unixfs.FolderPBData())
	node.SetCidBuilder(cid.V1Builder{
		Codec:    cid.DagProtobuf,
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	})
	for name, lnk := range links.Map() {
		if !lnk.Cid.Defined() {
			return PutResult{}, fmt.Errorf("cannot write link %q: empty CID", name)
		}
		node.AddRawLink(name, lnk.IPLD())
	}
	// _, err := node.EncodeProtobuf(false)
	err := mds.dagserv.Add(mds.ctx, node)
	if err != nil {
		return PutResult{}, err
	}
	size, err := node.Size()
	if err != nil {
		return PutResult{}, err
	}

	return PutResult{
		Cid:  node.Cid(),
		Size: int64(size),
	}, err
}

func (mds *merkleDagStore) GetBlock(ctx context.Context, id cid.Cid) ([]byte, error) {
	block, err := mds.bserv.GetBlock(ctx, id)
	if err != nil {
		return nil, err
	}
	return block.RawData(), nil
}

func (mds *merkleDagStore) PutBlock(d []byte) (id cid.Cid, err error) {
	mh, err := multihash.Sum(d, multihash.SHA2_256, -1)
	if err != nil {
		return cid.Cid{}, err
	}
	block, err := blocks.NewBlockWithCid(d, cid.NewCidV1(cid.Raw, mh))
	if err != nil {
		return cid.Cid{}, err
	}
	err = mds.bserv.AddBlock(block)
	return block.Cid(), err
}

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

type DagNode interface {
	Size() int64
	Cid() cid.Cid
	Links() Links
}

type dagNode struct {
	id   cid.Cid
	size int64
	node format.Node
}

var _ DagNode = (*dagNode)(nil)

func (n dagNode) Size() int64  { return n.size }
func (n dagNode) Cid() cid.Cid { return n.id }
func (n dagNode) Raw() []byte  { return n.node.RawData() }
func (n dagNode) Links() Links {
	links := NewLinks()
	for _, link := range n.node.Links() {
		links.Add(Link{
			Name: link.Name,
			Cid:  link.Cid,
			Size: int64(link.Size),
		})
	}
	return links
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

type Link struct {
	Name string
	Size int64
	Cid  cid.Cid

	IsFile bool
	Mtime  int64
}

func LinkFromNode(node DagNode, name string, isFile bool) Link {
	return Link{
		Name:   name,
		IsFile: isFile,

		Cid:  node.Cid(),
		Size: node.Size(),
	}
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
