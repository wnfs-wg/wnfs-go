package private

import (
	"context"
	"errors"
	"fmt"

	hamt "github.com/filecoin-project/go-hamt-ipld/v3"
	"github.com/ipfs/go-cid"
	base "github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/mdstore"
	ratchet "github.com/qri-io/wnfs-go/ratchet"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func Merge(aNode, bNode base.Node) (result base.MergeResult, err error) {
	dest, err := base.PrivateNodeFS(aNode)
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

	log.Debugw("Merge", "a", a, "b", b)
	return merge(dest, a, b)
}

func merge(destFS base.PrivateMerkleDagFS, a, b privateNode) (result base.MergeResult, err error) {
	acid := a.Cid()
	if a, ok := a.(*Root); ok {
		// TODO(b5): need to manually fetch cid from HAMT here b/c a.Cid() reports the
		// CID of the HAMT on the root, not the root dir
		apn, _ := a.PrivateName()
		if acid, err = cidFromPrivateName(a.PrivateFS(), apn); err != nil {
			log.Debugw("fetch a root CID", "err", err)
			return result, err
		}
	}

	bcid := b.Cid()
	if b, ok := b.(*Root); ok {
		// TODO(b5): need to manually fetch cid from HAMT here b/c a.Cid() reports the
		// CID of the HAMT on the root, not the root dir
		bpn, _ := b.PrivateName()
		bcid, err = cidFromPrivateName(b.PrivateFS(), bpn)
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
		if errors.Is(err, ratchet.ErrUnknownRatchetRelation) {
			return result, base.ErrNoCommonHistory
		}
		return result, err
	}

	log.Debugw("merge", "ratchetDistance", ratchetDistance, "a", a.Cid(), "b", b.Cid())
	if ratchetDistance == 0 {
		// ratchets are equal & cids are inequal, histories have diverged
		merged, err := mergeNodes(destFS, a, b, ratchetDistance)
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
		localCid, err := cidFromPrivateName(a.PrivateFS(), bPn)
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
		merged, err := mergeNodes(destFS, a, b, ratchetDistance)
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

	remoteCidAtLocalRatchetHead, err := cidFromPrivateName(b.PrivateFS(), pn)
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
	merged, err := mergeNodes(destFS, a, b, ratchetDistance)
	if err != nil {
		return result, err
	}
	return toMergeResult(merged, base.MTMergeCommit)
}

func mergeNodes(destFS base.PrivateMerkleDagFS, a, b privateNode, ratchetDistance int) (merged privateNode, err error) {
	// if b is preferred over a, switch values
	if ratchetDistance < 0 || (ratchetDistance == 0 && base.LessCID(b.Cid(), a.Cid())) {
		log.Debugw("mergeNodes, swapping b <-> a", "ratchetDistance", ratchetDistance, "bIsLess", base.LessCID(b.Cid(), a.Cid()))
		a, b = b, a
	}

	if err := mergeHamt(destFS.Context(), destFS.HAMT(), b.PrivateFS().HAMT()); err != nil {
		return nil, err
	}

	if root, ok := a.(*Root); ok {
		return mergeRoot(destFS, root, b)
	}

	aTree, aIsTree := a.(*Tree)
	bTree, bIsTree := b.(*Tree)
	if aIsTree && bIsTree {
		return mergeTrees(destFS, aTree, bTree)
	}

	return mergeNode(destFS, a, b)
}

func mergeHamt(ctx context.Context, dst, src *hamt.Node) error {
	log.Debugw("mergeHamt")
	return src.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		_, err := dst.SetIfAbsent(ctx, k, val)
		return err
	})
}

func mergeRoot(destfs base.PrivateMerkleDagFS, a *Root, b privateNode) (*Root, error) {
	var bTree *Tree
	switch t := b.(type) {
	case *Tree:
		bTree = t
	case *Root:
		bTree = t.Tree
	default:
		return nil, fmt.Errorf("expected a tree or root tree. got: %T", b)
	}

	if err := mergeHamt(destfs.Context(), destfs.HAMT(), b.PrivateFS().HAMT()); err != nil {
		return nil, err
	}

	mergedTree, err := mergeTrees(destfs, a.Tree, bTree)
	if err != nil {
		return nil, err
	}

	// TODO(b5): need to fully re-construct here including FS components to avoid
	// dragging a reference to the prior root as the internal filesystem
	// generally, the root shouldn't be implementing the FS.
	root := &Root{
		Tree:        mergedTree,
		ctx:         destfs.Context(),
		store:       destfs.PrivateStore(),
		hamt:        destfs.HAMT(),
		hamtRootCID: a.hamtRootCID,
	}

	putResult, err := root.Put()
	if err != nil {
		return nil, err
	}

	log.Debugw("mergeRoot", "res", putResult)
	return root, nil
}

func mergeTrees(destfs base.PrivateMerkleDagFS, a, b *Tree) (res *Tree, err error) {
	log.Debugw("mergeTrees", "a.name", a.name, "a", a.cid, "b", b.cid)
	for remName, remInfo := range b.info.Links {
		localInfo, existsLocally := a.info.Links[remName]

		if !existsLocally {
			// remote has a file local is missing. Add it.
			a.info.Links.Add(remInfo)
			continue
		}

		if localInfo.Cid.Equals(remInfo.Cid) {
			// both files are equal. no need to merge
			continue
		}

		// node exists in both trees & CIDs are inequal. merge recursively
		lcl, err := loadNodeFromPrivateLink(a.fs, localInfo)
		if err != nil {
			return res, err
		}
		rem, err := loadNodeFromPrivateLink(b.fs, remInfo)
		if err != nil {
			return res, err
		}

		res, err := merge(destfs, lcl, rem)
		if err != nil {
			return nil, err
		}

		key := &Key{}
		if err = key.Decode(res.Key); err != nil {
			return nil, err
		}

		l := PrivateLink{
			Link: mdstore.Link{
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
		a.info.Links.Add(l)
	}

	// TODO(b5): merge fields on private data
	// t.merge = &remote.cid
	a.info.Metadata.UnixMeta.Mtime = base.Timestamp().Unix()

	merged := &Tree{
		fs:      destfs,
		ratchet: a.ratchet,
		name:    a.name,
		info: TreeInfo{
			INum:  a.INumber(),
			Bnf:   a.info.Bnf,
			Links: a.info.Links,
			Metadata: &base.Metadata{
				UnixMeta: a.info.Metadata.UnixMeta,
				IsFile:   false,
				Version:  base.LatestVersion,
			},
		},
	}

	_, err = merged.Put()
	return merged, err
}

func mergeNode(destfs base.PrivateMerkleDagFS, a, b privateNode) (result privateNode, err error) {
	log.Debugw("mergeNode", "a", a.Cid(), "b", b.Cid())
	switch t := a.(type) {
	case *Tree:
		merged := &Tree{
			fs:      destfs,
			cid:     t.cid,
			info:    t.info,
			ratchet: t.ratchet,
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
			info:    t.info,
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

	// TODO(b5): factor away privateMerkleDagFS to make this accessible directly
	// from the passed in privateNode
	var hamtCid *cid.Cid
	if root, ok := n.(*Root); ok {
		hamtCid = root.hamtRootCID
	}

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

		HamtRoot:    hamtCid,
		PrivateName: string(pn),
		Key:         k.Encode(),
	}, nil
}
