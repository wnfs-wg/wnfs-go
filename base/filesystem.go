package base

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	multihash "github.com/multiformats/go-multihash"
)

var ErrNotFound = errors.New("not found")

const (
	// LatestVersion is the most recent semantic version of WNFS this
	// implementation reads/writes
	LatestVersion        = SemVer("2.0.0dev")
	DefaultMultihashType = multihash.SHA2_256
)

const (
	MetadataLinkName = "metadata"
	PublicLinkName   = "public"
	PrivateLinkName  = "private"
	SkeletonLinkName = "skeleton"
	PrettyLinkName   = "p"
	PreviousLinkName = "previous"
	MergeLinkName    = "merge"
	UserlandLinkName = "userland"
)

const (
	ModeDefault = 644
)

type NodeType uint8

const (
	NTFile NodeType = iota
	NTDataFile
	NTDir
	NTSymlink    // reserved for future use
	NTUnixFSFile // reserved for future use
	NTUnixFSDir  // reserved for future use
)

func (nt NodeType) String() string {
	switch nt {
	case NTFile:
		return "file"
	case NTDataFile:
		return "datafile"
	case NTDir:
		return "dir"
	case NTSymlink:
		return "symlink"
	case NTUnixFSFile:
		return "unixFSFile"
	case NTUnixFSDir:
		return "unixFSDir"
	default:
		return "unknown"
	}
}

type SemVer string

var ErrNoLink = fmt.Errorf("no link")

type Node interface {
	fs.File
	fs.FileInfo
	Cid() cid.Cid
	Type() NodeType
	AsHistoryEntry() HistoryEntry
	History(ctx context.Context, limit int) ([]HistoryEntry, error)
	Meta() (LinkedDataFile, error)
}

type File interface {
	Node
}

type LinkedDataFile interface {
	fs.File
	fs.ReadDirFile
	Data() (interface{}, error)
}

type Tree interface {
	Node
	fs.ReadDirFile
	Get(path Path) (fs.File, error)
	Add(path Path, f fs.File) (PutResult, error)
	Copy(path Path, srcPath string, src fs.FS) (PutResult, error)
	Rm(path Path) (PutResult, error)
	Mkdir(path Path) (PutResult, error)
}

type PutResult interface {
	CID() cid.Cid
	ToLink(name string) Link
	// ToSkeletonInfo() SkeletonInfo
}

type Header interface {
	Previous() *cid.Cid
}

// type TreeHeader interface {
// 	Header
// 	Skeleton() Skeleton
// }

// info is header data + a userland CID
type Info interface {
	Header
	Userland() cid.Cid
}

type HistoryEntry struct {
	Cid      cid.Cid  `json:"cid"`
	Previous *cid.Cid `json:"previous"`
	Type     NodeType `json:"type"`
	Mtime    int64    `json:"mtime"`
	Size     int64    `json:"size"`

	Key         string `json:"key,omitempty"`
	PrivateName string `json:"privateName,omitempty"`
}

func Filename(file fs.File) (string, error) {
	fi, err := file.Stat()
	if err != nil {
		return "", err
	}
	return fi.Name(), nil
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
