package base

import (
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
	UserlandLinkName = "userland"
)

// LatestVersion is the most recent semantic version of WNFS this implementation
// reads/writes
const LatestVersion = SemVer("2.0.0dev")

type MerkleDagFS interface {
	fs.FS
	DagStore() mdstore.MerkleDagStore
	HAMT() *hamt.Node
}

type Node interface {
	fs.File
	AsHistoryEntry() HistoryEntry
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

func Filename(file fs.File) (string, error) {
	fi, err := file.Stat()
	if err != nil {
		return "", err
	}
	return fi.Name(), nil
}
