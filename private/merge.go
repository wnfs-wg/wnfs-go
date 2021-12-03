package private

import (
	"context"
	"errors"
	"fmt"

	base "github.com/qri-io/wnfs-go/base"
	ratchet "github.com/qri-io/wnfs-go/ratchet"
)

func Merge(ctx context.Context, aNode, bNode base.Node) (result base.MergeResult, err error) {
	dstStore, err := NodeStore(aNode)
	if err != nil {
		return result, err
	}
	srcStore, err := NodeStore(bNode)
	if err != nil {
		return result, err
	}
	a, ok := aNode.(privateNode)
	if !ok {
		return result, fmt.Errorf("cannot merge. Node must be private")
	}
	b, ok := bNode.(privateNode)
	if !ok {
		return result, fmt.Errorf("cannot merge. Node must be private")
	}

	err = MergeHAMTBlocks(ctx, srcStore, dstStore)
	if err != nil {
		return result, err
	}

	log.Debugw("Merge", "a", a.Cid(), "b", b.Cid())
	result, err = merge(ctx, dstStore, a, b)
	if err != nil {
		return result, err
	}
	return result, err
}

func merge(ctx context.Context, destFS Store, a, b privateNode) (result base.MergeResult, err error) {
	acid := a.Cid()
	if a, ok := a.(*Root); ok {
		// TODO(b5): need to manually fetch cid from HAMT here b/c a.Cid() reports the
		// CID of the HAMT on the root, not the root dir
		apn, _ := a.PrivateName()
		if acid, err = cidFromPrivateName(ctx, a.PrivateFS(), apn); err != nil {
			log.Debugw("fetch a root CID", "err", err)
			return result, err
		}
	}

	bcid := b.Cid()
	if b, ok := b.(*Root); ok {
		// TODO(b5): need to manually fetch cid from HAMT here b/c a.Cid() reports the
		// CID of the HAMT on the root, not the root dir
		bpn, _ := b.PrivateName()
		bcid, err = cidFromPrivateName(ctx, b.PrivateFS(), bpn)
		if err != nil {
			log.Debugw("fetch b root CID", "err", err)
			return result, err
		}
	}

	if acid.Equals(bcid) {
		// head cids are equal, in sync
		fi, err := a.Stat()
		if err != nil {
			return result, err
		}

		pn, err := a.PrivateName()
		if err != nil {
			return result, err
		}
		k := Key(a.Ratchet().Key())

		return base.MergeResult{
			Type:   base.MTInSync,
			Cid:    a.Cid(),
			Size:   fi.Size(),
			IsFile: !fi.IsDir(),
			Name:   fi.Name(),
			// TODO (b5): private fields
			PrivateName: string(pn),
			Key:         k.Encode(),
		}, nil
	}

	ratchetDistance, err := a.Ratchet().Compare(*b.Ratchet(), 100000)
	if err != nil {
		log.Debugw("comparing ratchets", "a", a.Ratchet().Summary(), "b", b.Ratchet().Summary(), "err", err)
		if errors.Is(err, ratchet.ErrUnknownRatchetRelation) {
			return result, base.ErrNoCommonHistory
		}
		return result, err
	}

	aStore, err := NodeStore(a)
	if err != nil {
		return result, err
	}
	bStore, err := NodeStore(b)
	if err != nil {
		return result, err
	}

	log.Debugw("merge", "ratchetDistance", ratchetDistance, "a", a.Cid(), "b", b.Cid())
	if ratchetDistance == 0 {
		// ratchets are equal & cids are inequal, histories have diverged
		merged, err := mergeDivergedNodes(ctx, destFS, a, b, ratchetDistance)
		if err != nil {
			return result, err
		}
		return toMergeResult(merged, base.MTMergeCommit)

	} else if ratchetDistance > 0 {
		// local ratchet is ahead of remote, fetch HAMT CID for remote ratchet head
		// and compare to local
		bPn, err := b.PrivateName()
		if err != nil {
			return result, err
		}
		localCid, err := cidFromPrivateName(ctx, aStore, bPn)
		if err != nil {
			return result, err
		}

		if localCid.Equals(bcid) {
			// cids at matching ratchet positions are equal, local is strictly ahead

			fi, err := a.Stat()
			if err != nil {
				return result, err
			}

			pn, err := b.PrivateName()
			if err != nil {
				return result, err
			}
			k := Key(b.Ratchet().Key())

			return base.MergeResult{
				Type: base.MTLocalAhead,

				Name:   fi.Name(),
				Cid:    a.Cid(),
				Size:   fi.Size(),
				IsFile: !fi.IsDir(),

				PrivateName: string(pn),
				Key:         k.Encode(),
			}, nil
		}

		// cids at matching ratchet positions are inequal, histories have diverged
		merged, err := mergeDivergedNodes(ctx, destFS, a, b, ratchetDistance)
		if err != nil {
			return result, err
		}
		return toMergeResult(merged, base.MTMergeCommit)
	}

	// ratchetDistance < 0
	// remote ratchet is ahead of local, fetch CID for local ratchet head
	// from remote and compare to local
	pn, err := a.PrivateName()
	if err != nil {
		return base.MergeResult{}, err
	}

	remoteCidAtLocalRatchetHead, err := cidFromPrivateName(ctx, bStore, pn)
	if err != nil {
		return base.MergeResult{}, err
	}

	if acid.Equals(remoteCidAtLocalRatchetHead) {
		// cids at matching ratchet positions are equal, remote is strictly ahead
		fi, err := b.Stat()
		if err != nil {
			return result, err
		}

		pn, err := b.PrivateName()
		if err != nil {
			return result, err
		}
		k := Key(b.Ratchet().Key())

		return base.MergeResult{
			Type: base.MTFastForward,

			Name: fi.Name(),
			Cid:  b.Cid(),
			// TODO(b5):
			Size:   fi.Size(),
			IsFile: !fi.IsDir(),

			PrivateName: string(pn),
			Key:         k.Encode(),
		}, nil
	}

	// cids at matching ratchet positions are inequal, histories have diverged
	merged, err := mergeDivergedNodes(ctx, destFS, a, b, ratchetDistance)
	if err != nil {
		return result, err
	}
	return toMergeResult(merged, base.MTMergeCommit)
}

func mergeDivergedNodes(ctx context.Context, destFS Store, a, b privateNode, ratchetDistance int) (merged privateNode, err error) {
	// if b is preferred over a, switch values
	if ratchetDistance < 0 || (ratchetDistance == 0 && base.LessCID(b.Cid(), a.Cid())) {
		log.Debugw("mergeDivergedNodes, swapping b <-> a", "ratchetDistance", ratchetDistance, "bIsLess", base.LessCID(b.Cid(), a.Cid()))
		a, b = b, a
	}

	bStore, err := NodeStore(b)
	if err != nil {
		return nil, err
	}

	if err := destFS.HAMT().Merge(ctx, bStore.HAMT().Root()); err != nil {
		return nil, err
	}

	if root, ok := a.(*Root); ok {
		return mergeDivergedRoot(ctx, destFS, root, b)
	}

	aTree, aIsTree := a.(*Tree)
	bTree, bIsTree := b.(*Tree)
	if aIsTree && bIsTree {
		return mergeDivergedTrees(ctx, destFS, aTree, bTree)
	}

	return mergeDivergedNode(ctx, destFS, a, b)
}

func mergeDivergedRoot(ctx context.Context, destfs Store, a *Root, b privateNode) (*Root, error) {
	var bTree *Tree
	switch t := b.(type) {
	case *Tree:
		bTree = t
	case *Root:
		bTree = t.Tree
	default:
		return nil, fmt.Errorf("expected a tree or root tree. got: %T", b)
	}

	bStore, err := NodeStore(b)
	if err != nil {
		return nil, err
	}

	if err := destfs.HAMT().Merge(ctx, bStore.HAMT().Root()); err != nil {
		return nil, err
	}

	mergedTree, err := mergeDivergedTrees(ctx, destfs, a.Tree, bTree)
	if err != nil {
		return nil, err
	}

	root := &Root{
		// TODO(b5): using the wrong context here:
		ctx:  ctx,
		Tree: mergedTree,
	}

	putResult, err := root.Put()
	if err != nil {
		return nil, err
	}

	log.Debugw("mergeDivergedRoot", "res", putResult)
	return root, nil
}

func mergeDivergedTrees(ctx context.Context, destfs Store, a, b *Tree) (res *Tree, err error) {
	log.Debugw("mergeDivergedTrees", "a.name", a.name, "a", a.cid, "b", b.cid)
	checked := map[string]struct{}{}

	for remName, remInfo := range b.links {
		localInfo, existsLocally := a.links[remName]

		if !existsLocally {
			// remote has a file local is missing. Add it.
			log.Debugw("adding missing remote file", "name", remName, "cid", remInfo.Cid)
			a.links.Add(remInfo)
			checked[remName] = struct{}{}
			continue
		}

		if localInfo.Cid.Equals(remInfo.Cid) {
			// both files are equal. no need to merge
			checked[remName] = struct{}{}
			continue
		}

		// node exists in both trees & CIDs are inequal. merge recursively
		lcl, err := LoadNode(ctx, a.fs, localInfo.Name, localInfo.Cid, localInfo.Key)
		if err != nil {
			return res, err
		}
		rem, err := LoadNode(ctx, b.fs, remInfo.Name, remInfo.Cid, remInfo.Key)
		if err != nil {
			return res, err
		}

		res, err := merge(ctx, destfs, lcl, rem)
		if err != nil {
			return nil, err
		}

		key := &Key{}
		if err = key.Decode(res.Key); err != nil {
			return nil, err
		}

		l := PrivateLink{
			Link: base.Link{
				Name:   res.Name,
				Size:   res.Size,
				Cid:    res.Cid,
				IsFile: res.IsFile,
				// TODO(b5): audit fields
			},
			Key:     *key,
			Pointer: Name(res.PrivateName),
		}
		log.Debugw("adding link", "link", l)
		a.links.Add(l)
		checked[remName] = struct{}{}
	}

	// iterate all of a's files making sure they're present on destFS
	for aName, aInfo := range a.links {
		if _, ok := checked[aName]; !ok {
			log.Debugw("copying blocks for file", "name", aName, "cid", aInfo.Cid)
			// if err := CopyBlocks(ctx, aInfo.Cid, a.fs, destfs); err != nil {
			// 	return nil, err
			// }
		}
	}

	a.header.Info.Mtime = base.Timestamp().Unix()

	merged := &Tree{
		fs:      destfs,
		ratchet: a.ratchet,
		name:    a.name,
		links:   a.links,
		header: Header{
			Info: a.header.Info,
		},
	}

	_, err = merged.Put()
	return merged, err
}

func mergeDivergedNode(ctx context.Context, destfs Store, a, b privateNode) (result privateNode, err error) {
	log.Debugw("mergeDivergedNode", "a", a.Cid(), "b", b.Cid())

	// bStore, err := NodeStore(a)
	// if err != nil {
	// 	return nil, err
	// }
	// if err = CopyBlocks(ctx, a.Cid(), bStore, destfs); err != nil {
	// 	return nil, err
	// }

	switch t := a.(type) {
	case *Tree:
		merged := &Tree{
			fs:      destfs,
			cid:     t.cid,
			header:  t.header,
			ratchet: t.ratchet,
			links:   t.links,
			name:    t.name,
		}
		_, err = merged.Put()
		return merged, err
	case *File:
		if err = t.ensureContent(); err != nil {
			return nil, err
		}

		merged := &File{
			fs:      destfs,
			ratchet: t.ratchet,
			header:  t.header,
			name:    t.name,
			cid:     t.cid,
			content: t.content,
		}
		_, err = merged.Put()
		return merged, err
	default:
		return nil, fmt.Errorf("unexpected node type for private merge: %T", a)
	}
}

func toMergeResult(n privateNode, mt base.MergeType) (result base.MergeResult, err error) {
	log.Debugw("toMergeResult", "node", fmt.Sprintf("%T", n))

	store, err := NodeStore(n)
	if err != nil {
		return result, err
	}
	hamtCID := store.HAMT().CID()

	fi, err := Stat(n)
	if err != nil {
		return result, err
	}

	pn, err := n.PrivateName()
	if err != nil {
		return result, err
	}
	k := Key(n.Ratchet().Key())

	return base.MergeResult{
		Name:   fi.Name(),
		Type:   mt,
		Cid:    fi.Cid(),
		Size:   fi.Size(),
		IsFile: !fi.IsDir(),

		HamtRoot:    &hamtCID,
		PrivateName: string(pn),
		Key:         k.Encode(),
	}, nil
}
