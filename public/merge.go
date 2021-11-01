package public

import (
	"fmt"
	"time"

	base "github.com/qri-io/wnfs-go/base"
)

// 1. commits have diverged.
// 2. pick winner:
// 	* if "A" is winner "merge" value will be "B" head
// 	* if "B" is winner "merge value will be "A" head
// 	* in both cases the result itself to be a new CID
// 3. perform merge:
// 	* if both are directories, merge recursively
// 	* in all other cases, replace prior contents with winning CID
func mergeNodes(a, b base.Node, aGen, bGen int) (merged base.Node, err error) {
	// TODO(b5): oh this is SO broken
	fs := a.(*PublicFile).Store()
	// if b is preferred over a, switch values
	if aGen < bGen || (aGen == bGen && base.LessCID(b.Cid(), a.Cid())) {
		a, b = b, a
	}

	aTree, aIsTree := a.(*PublicTree)
	bTree, bIsTree := b.(*PublicTree)
	if aIsTree && bIsTree {
		return mergeTrees(fs, aTree, bTree)
	}

	return mergeNode(fs, a, b)
}

func mergeTrees(dest base.MerkleDagFS, a, b *PublicTree) (*PublicTree, error) {
	return nil, fmt.Errorf("unfinished: mergeTrees")
}

// construct a new node from a, with merge field set to b.Cid, store new node on
// dest
func mergeNode(dest base.MerkleDagFS, a, b base.Node) (merged base.Node, err error) {
	bid := b.Cid()

	switch a := a.(type) {
	case *PublicTree:
		h := &header{
			Merge:    &bid,
			Previous: &a.h.cid,
			Size:     a.h.Size,
			Metadata: a.h.Metadata,
			Skeleton: a.h.Skeleton,
			Userland: a.h.Userland,
		}

		if err = h.write(dest); err != nil {
			return nil, err
		}

		tree := &PublicTree{
			fs:   dest,
			name: a.name,
			h:    h,

			metadata: a.metadata,
			skeleton: a.skeleton,
			userland: a.userland,
		}

		tree.metadata.UnixMeta.Mtime = time.Now().Unix()
		_, err := a.Put()
		return tree, err

	case *PublicFile:
		file := &PublicFile{
			fs:    dest,
			name:  a.Name(),
			cid:   a.Cid(),
			merge: &bid,
		}

		_, err = file.Put()
		return file, err
	default:
		return nil, fmt.Errorf("unknown type merging node %T", a)
	}
}
