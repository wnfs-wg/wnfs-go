package base

import (
	"context"
	"fmt"
	"io/fs"

	hamt "github.com/filecoin-project/go-hamt-ipld/v3"
	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

const (
	MetadataLinkName = "metadata"
	SkeletonLinkName = "skeleton"
	PrettyLinkName   = "p"
	PreviousLinkName = "previous"
	MergeLinkName    = "merge"
	UserlandLinkName = "userland"
)

// LatestVersion is the most recent semantic version of WNFS this implementation
// reads/writes
const LatestVersion = SemVer("2.0.0dev")

type MerkleDagFS interface {
	fs.FS
	Context() context.Context
	DagStore() mdstore.MerkleDagStore
}

type PrivateMerkleDagFS interface {
	Context() context.Context
	HAMT() *hamt.Node
	PrivateStore() mdstore.PrivateStore
}

type Node interface {
	fs.File
	AsHistoryEntry() HistoryEntry
	AsLink() mdstore.Link

	// Merge a remote node, assumed node is aligned between local & remote tree,
	// and two nodes have a diverged history
	MergeDiverged(remote Node) (MergeResult, error)
}

type File interface {
	Node
}

type Tree interface {
	Node
	Get(path Path) (fs.File, error)
	Add(path Path, f fs.File) (PutResult, error)
	Copy(path Path, srcPath string, src fs.FS) (PutResult, error)
	Rm(path Path) (PutResult, error)
	Mkdir(path Path) (PutResult, error)

	History(path Path, limit int) ([]HistoryEntry, error)
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
	Cid      cid.Cid   `json:"cid"`
	Previous *cid.Cid  `json:"previous"`
	Metadata *Metadata `json:"metadata"`
	Size     int64     `json:"size"`
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
