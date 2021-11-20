package base

import (
	"context"
	"fmt"
	"io/fs"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
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

// LatestVersion is the most recent semantic version of WNFS this implementation
// reads/writes
const LatestVersion = SemVer("2.0.0dev")

var ErrNoLink = fmt.Errorf("no link")

type MerkleDagFS interface {
	fs.FS
	Context() context.Context
	DagStore() mdstore.MerkleDagStore
}

type Node interface {
	fs.File
	Cid() cid.Cid
	AsHistoryEntry() HistoryEntry
	AsLink() mdstore.Link
	History(ctx context.Context, limit int) ([]HistoryEntry, error)
}

type File interface {
	Node
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
	ToLink(name string) mdstore.Link
	ToSkeletonInfo() SkeletonInfo
}

type Header interface {
	Metadata() Metadata
	Previous() *cid.Cid
}

type TreeHeader interface {
	Header
	Skeleton() Skeleton
}

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

func NodeFS(n Node) (MerkleDagFS, error) {
	st, err := n.Stat()
	if err != nil {
		return nil, err
	}
	mdfs, ok := st.Sys().(MerkleDagFS)
	if !ok {
		return nil, fmt.Errorf("node Sys is not a MerkleDagFS")
	}
	return mdfs, nil
}
