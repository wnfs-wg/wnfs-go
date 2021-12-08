package public

import (
	"context"
	"fmt"
	"time"

	base "github.com/qri-io/wnfs-go/base"
)

func Merge(ctx context.Context, a, b base.Node) (result base.MergeResult, err error) {
	dest, err := NodeStore(a)
	if err != nil {
		return result, err
	}
	return merge(ctx, dest, a, b)
}

func merge(ctx context.Context, destStore Store, a, b base.Node) (result base.MergeResult, err error) {
	var (
		aCur, bCur   = a, b
		aHist, bHist = a.AsHistoryEntry(), b.AsHistoryEntry()
		aGen, bGen   = 0, 0
	)

	aStat, _ := a.Stat()
	bStat, _ := b.Stat()
	log.Debugf("merge afs: %#v, bfs: %#v\n", aStat.Sys(), bStat.Sys())

	// check for equality first
	if aHist.Cid.Equals(bHist.Cid) {
		return base.MergeResult{
			Type: base.MTInSync,
			Cid:  aHist.Cid,
			// Userland: aHist.Userland,
			// Metadata: bHist.Metadata,
			Size:   aHist.Size,
			IsFile: aHist.Type == base.NTFile,
		}, nil
	}

	afs, err := NodeStore(a)
	if err != nil {
		return result, err
	}
	bfs, err := NodeStore(b)
	if err != nil {
		return result, err
	}

	for {
		bCur = b
		bGen = 0
		bHist = b.AsHistoryEntry()
		for {
			if aHist.Cid.Equals(bHist.Cid) {
				if aGen == 0 && bGen > 0 {
					// fast-forward
					bHist = b.AsHistoryEntry()
					return base.MergeResult{
						Type: base.MTFastForward,
						// TODO(b5):
						// 	Userland: si.Cid,
						// 	Metadata: si.Metadata,
						Cid:    bHist.Cid,
						Size:   bHist.Size,
						IsFile: bHist.Type == base.NTFile,
					}, nil
				} else if aGen > 0 && bGen == 0 {
					result.Type = base.MTLocalAhead
					aHist := a.AsHistoryEntry()
					return base.MergeResult{
						Type:   base.MTLocalAhead,
						Cid:    aHist.Cid,
						Size:   aHist.Size,
						IsFile: bHist.Type == base.NTFile,
					}, nil
				} else {
					// both local & remote are greater than zero, have diverged
					merged, err := mergeNodes(ctx, destStore, a, b, aGen, bGen)
					if err != nil {
						return result, err
					}
					mergedStat, err := base.Stat(a)
					if err != nil {
						return result, err
					}
					return base.MergeResult{
						Type:   base.MTMergeCommit,
						Cid:    merged.Cid(),
						IsFile: !mergedStat.IsDir(),
					}, nil
				}
			}

			if bHist.Previous == nil {
				break
			}
			name, err := base.Filename(bCur)
			if err != nil {
				return result, err
			}
			bCur, err = loadNode(ctx, bfs, name, *bHist.Previous)
			if err != nil {
				return result, err
			}
			bHist = bCur.AsHistoryEntry()
			bGen++
		}

		if aHist.Previous == nil {
			break
		}
		name, err := base.Filename(aCur)
		if err != nil {
			return result, err
		}
		aCur, err = loadNode(ctx, afs, name, *aHist.Previous)
		if err != nil {
			return result, err
		}
		aHist = aCur.AsHistoryEntry()
		aGen++
	}

	// no common history, merge based on heigh & alpha-sorted-cid
	merged, err := mergeNodes(ctx, destStore, a, b, aGen, bGen)
	if err != nil {
		return result, err
	}
	mergedStat, err := base.Stat(a)
	if err != nil {
		return result, err
	}

	return base.MergeResult{
		Type:   base.MTMergeCommit,
		Cid:    merged.Cid(),
		IsFile: !mergedStat.IsDir(),
	}, nil
}

// 1. commits have diverged.
// 2. pick winner:
// 	* if "A" is winner "merge" value will be "B" head
// 	* if "B" is winner "merge value will be "A" head
// 	* in both cases the result itself to be a new CID
// 3. perform merge:
// 	* if both are directories, merge recursively
// 	* in all other cases, replace prior contents with winning CID
// always writes to a's filesystem
func mergeNodes(ctx context.Context, destStore Store, a, b base.Node, aGen, bGen int) (merged base.Node, err error) {
	log.Debugw("merge nodes", "aName", a.Name(), "bName", b.Name(), "destStore", fmt.Sprintf("%#v", destStore))
	// if b is preferred over a, switch values
	if aGen < bGen || (aGen == bGen && base.LessCID(b.Cid(), a.Cid())) {
		a, b = b, a
	}

	aTree, aIsTree := a.(*Tree)
	bTree, bIsTree := b.(*Tree)
	if aIsTree && bIsTree {
		return mergeTrees(ctx, destStore, aTree, bTree)
	}

	return mergeNode(ctx, destStore, a, b)
}

func mergeTrees(ctx context.Context, destStore Store, a, b *Tree) (*Tree, error) {
	log.Debugw("mergeTrees", "a_skeleton", a.skeleton)
	checked := map[string]struct{}{}

	for remName, remInfo := range b.skeleton {
		localInfo, existsLocally := a.skeleton[remName]
		log.Debugw("merging trees", "name", remName, "existsLocally", existsLocally)

		if !existsLocally {
			// remote has a file local is missing, add it.
			n, err := loadNodeFromSkeletonInfo(ctx, b.store, remName, remInfo)
			if err != nil {
				return nil, err
			}
			log.Debugw("mergeTrees add file", "dir", a.Name(), "file", remName, "cid", n.Cid())

			if err := base.CopyBlocks(destStore.Context(), n.Cid(), b.store.Blockservice(), destStore.Blockservice()); err != nil {
				return nil, err
			}

			a.skeleton[remName] = remInfo
			a.userland.Add(base.Link{
				Name:   n.Name(),
				Size:   n.Size(),
				Cid:    n.Cid(),
				Mtime:  n.ModTime().Unix(),
				IsFile: (n.Type() == base.NTFile || n.Type() == base.NTLDFile),
			})
			checked[remName] = struct{}{}
			continue
		}

		if localInfo.Cid.Equals(remInfo.Cid) {
			// both files are equal. no need to merge
			checked[remName] = struct{}{}
			continue
		}

		// node exists in both trees & CIDs are inequal. merge recursively
		lcl, err := loadNodeFromSkeletonInfo(ctx, a.store, remName, localInfo)
		if err != nil {
			return nil, err
		}
		rem, err := loadNodeFromSkeletonInfo(ctx, b.store, remName, remInfo)
		if err != nil {
			return nil, err
		}

		res, err := merge(ctx, destStore, lcl, rem)
		if err != nil {
			return nil, err
		}
		a.skeleton[remName] = mergeResultToSkeletonInfo(res)
		a.userland.Add(res.ToLink(remName))
		checked[remName] = struct{}{}
	}

	// iterate all of a's files making sure they're present on destStore
	for aName, aInfo := range a.skeleton {
		if _, ok := checked[aName]; !ok {
			log.Debugw("copying blocks for a file", "name", aName, "cid", aInfo.Cid)
			if err := base.CopyBlocks(destStore.Context(), aInfo.Cid, a.store.Blockservice(), destStore.Blockservice()); err != nil {
				return nil, err
			}
		}
	}

	a.h.Merge = &b.cid
	a.h.Info.Mtime = base.Timestamp().Unix()
	a.store = destStore
	if _, err := a.Put(); err != nil {
		return nil, err
	}
	return a, nil
}

// construct a new node from a, with merge field set to b.Cid, store new node on
// dest
func mergeNode(ctx context.Context, destStore Store, a, b base.Node) (merged base.Node, err error) {
	bid := b.Cid()

	switch a := a.(type) {
	case *Tree:
		tree := &Tree{
			store: destStore,
			name:  a.name,
			cid:   a.cid,
			h: &Header{
				Info:     a.h.Info,
				Merge:    &bid,
				Previous: &a.cid,
				Metadata: a.h.Metadata,
				Skeleton: a.h.Skeleton,
				Userland: a.h.Userland,
			},
			metadata: a.metadata,
			skeleton: a.skeleton,
			userland: a.userland,
		}

		tree.h.Info.Mtime = time.Now().Unix()
		_, err := a.Put()
		return tree, err

	case *File:
		if err = a.ensureContent(); err != nil {
			return nil, err
		}

		return &File{
			store: destStore,
			name:  a.Name(),
			cid:   a.cid,
			h: &Header{
				Info:     a.h.Info,
				Merge:    &bid,
				Previous: &a.cid,
				Metadata: a.h.Metadata,
				Skeleton: a.h.Skeleton,
				Userland: a.h.Userland,
			},
			metadata: a.metadata,
			content:  a.content,
		}, nil
	default:
		return nil, fmt.Errorf("unknown type merging node %T", a)
	}
}
