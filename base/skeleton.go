package base

import (
	"context"
	"io/fs"
	"io/ioutil"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

type SkeletonInfo struct {
	Cid         cid.Cid  `json:"cid,omitempty"`
	Userland    cid.Cid  `json:"userland,omitempty"`
	Metadata    cid.Cid  `json:"metadata,omitempty"`
	SubSkeleton Skeleton `json:"subSkeleton,omitempty"`
	IsFile      bool     `json:"isFile"`
}

type Skeleton map[string]SkeletonInfo

func LoadSkeleton(ctx context.Context, store mdstore.MerkleDagStore, id cid.Cid) (Skeleton, error) {
	f, err := store.GetFile(ctx, id)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	d, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	sk := Skeleton{}
	return sk, DecodeCBOR(d, &sk)
}

func (s Skeleton) CBORFile() (fs.File, error) {
	buf, err := EncodeCBOR(s)
	if err != nil {
		return nil, err
	}

	// TODO(b5): use bareFile instead?
	return &memfile{
		fi: &FSFileInfo{
			name:  SkeletonLinkName,
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

type PrivateSkeletonInfo struct {
	Id          cid.Cid         `json:"id"`
	Key         string          `json:"key"`
	SubSkeleton PrivateSkeleton `json:"subSkeleton"`
}

type PrivateSkeleton map[string]PrivateSkeletonInfo

func LoadPrivateSkeleton(ctx context.Context, store mdstore.MerkleDagStore, id cid.Cid, key string) (PrivateSkeleton, error) {
	f, err := store.GetFile(ctx, id)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	d, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	sk := PrivateSkeleton{}
	return sk, DecodeCBOR(d, &sk)
}

func (ps PrivateSkeleton) CBORFile(key *string) (fs.File, error) {
	buf, err := EncodeCBOR(ps)
	if err != nil {
		return nil, err
	}

	// TODO(b5): use bareFile instead?
	return &memfile{
		fi: &FSFileInfo{
			name:  SkeletonLinkName,
			size:  int64(buf.Len()),
			mode:  0755,
			mtime: Timestamp(),
		},
		buf: buf,
	}, nil
}
