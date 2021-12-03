package base

import (
	"context"
	"fmt"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbornode "github.com/ipfs/go-ipld-cbor"
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
	Name     string
	Type     MergeType
	Cid      cid.Cid // finalized (possibly updated) CID
	Userland cid.Cid
	Metadata cid.Cid
	Size     int64
	IsFile   bool

	HamtRoot    *cid.Cid // TODO(b5): refactor this away. unused on public nodes, required for private
	Key         string
	PrivateName string
}

var _ PutResult = (*MergeResult)(nil)

func (mr MergeResult) CID() cid.Cid {
	return mr.Cid
}

func (mr MergeResult) ToLink(name string) Link {
	return Link{
		Name:   name,
		Cid:    mr.Cid,
		Size:   mr.Size,
		IsFile: mr.IsFile,
	}
}

func LessCID(a, b cid.Cid) bool {
	return a.String() > b.String()
}

// Copy blocks from src to dst
func CopyBlocks(ctx context.Context, id cid.Cid, src, dst blockservice.BlockService) error {
	blk, err := src.GetBlock(ctx, id)
	if err != nil {
		return err
	}

	if blk.Cid().Type() == cid.DagCBOR {
		n, err := cbornode.DecodeBlock(blk)
		if err != nil {
			return err
		}
		for _, l := range n.Links() {
			if err := CopyBlocks(ctx, l.Cid, src, dst); err != nil {
				return fmt.Errorf("copying block %q: %w", l.Cid, err)
			}
		}
	}

	return dst.Blockstore().Put(blk)
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
