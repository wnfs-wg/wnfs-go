package wnfs

import (
	"fmt"
	"io/fs"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

type SkeletonInfo struct {
	Cid      cid.Cid `json:"cid"`
	Userland cid.Cid `json:"userland"`
	// Metadata    cid.Cid  `json:"metadata"`
	SubSkeleton Skeleton `json:"subSkeleton"`
	IsFile      bool     `json:"isFile"`
}

type Skeleton map[string]SkeletonInfo

func loadSkeleton(store mdstore.MerkleDagStore, id cid.Cid) (Skeleton, error) {
	d, err := mdstore.GetBlockBytes(store, id)
	if err != nil {
		return nil, fmt.Errorf("getting block bytes:\n%w", err)
	}

	sk := Skeleton{}
	err = decodeCBOR(d, &sk)
	return sk, nil
}

func (s Skeleton) CBORFile(key *string) (fs.File, error) {
	buf, err := encodeCBOR(s, key)
	if err != nil {
		return nil, err
	}

	// TODO(b5): use bareFile instead?
	return &memfile{
		fi: &fsFileInfo{
			name:  skeletonLinkName,
			size:  int64(buf.Len()),
			mode:  0755,
			mtime: Timestamp(),
		},
		buf: buf,
	}, nil
}

func (s Skeleton) PathInfo(path Path) (SkeletonInfo, error) {
	head, tail := path.Shift()
	info, ok := s[head]
	if !ok {
		return info, ErrNotFound
	}

	if tail != nil {
		return info.SubSkeleton.PathInfo(tail)
	}
	return info, nil
}
