package base

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

// ErrNoCommonHistory signifies a merge error where two nodes share no common
// history
var ErrNoCommonHistory = fmt.Errorf("no common history")

type MergeType string

const (
	MTInSync      MergeType = "in-sync"
	MTLocalAhead  MergeType = "local-ahead-of-remote"
	MTFastForward MergeType = "fast-forward"
	MTMergeCommit MergeType = "merge-commit"
)

type MergeResult struct {
	Type     MergeType
	Cid      cid.Cid // finalized (possibly updated) CID
	Userland cid.Cid
	Metadata cid.Cid
	Size     int64
	IsFile   bool
}

var _ PutResult = (*MergeResult)(nil)

func (mr MergeResult) CID() cid.Cid {
	return mr.Cid
}

func (mr MergeResult) ToLink(name string) mdstore.Link {
	return mdstore.Link{
		Name:   name,
		Cid:    mr.Cid,
		Size:   mr.Size,
		IsFile: mr.IsFile,
	}
}

func (mr MergeResult) ToSkeletonInfo() SkeletonInfo {
	return SkeletonInfo{
		Cid:         mr.Cid,
		Userland:    mr.Userland,
		Metadata:    mr.Metadata,
		SubSkeleton: nil,
		IsFile:      mr.IsFile,
	}
}

func LessCID(a, b cid.Cid) bool {
	return a.String() > b.String()
}
