package mdstore

import (
	"io"
	"io/fs"
	"io/ioutil"
	"sort"

	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
)

// MerkleDagStore is a store of Content-Addressed block data indexed by merkle
// proofs. It's an abstraction over IPFS that defines the required feature set
// to run wnfs
type MerkleDagStore interface {
	StoreType() string

	// linked data nodes
	GetNode(id cid.Cid, path ...string) (DagNode, error)
	PutNode(links Links) (PutResult, error)

	GetBlock(id cid.Cid) (r io.Reader, err error)
	PutBlock(d []byte) (id cid.Cid, err error)

	// files
	PutFile(f fs.File) (PutResult, error)
	GetFile(root cid.Cid, path ...string) (io.ReadCloser, error)

	PutEncryptedFile(f fs.File, key []byte) (PutResult, error)
	GetEncryptedFile(root cid.Cid, key []byte) (io.ReadCloser, error)
}

func GetBlockBytes(store MerkleDagStore, id cid.Cid) ([]byte, error) {
	r, err := store.GetBlock(id)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

type DagNode interface {
	Size() int64
	Cid() cid.Cid
	Links() Links
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
