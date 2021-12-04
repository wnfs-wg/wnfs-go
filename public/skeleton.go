package public

import (
	"context"
	"io"
	"io/fs"
	"io/ioutil"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/base"
)

type SkeletonSource interface {
	Skeleton() (Skeleton, error)
}

type FileGetter interface {
	GetFile(context.Context, cid.Cid) (io.ReadCloser, error)
}

type SkeletonInfo struct {
	Cid         cid.Cid  `json:"cid,omitempty"`
	Userland    cid.Cid  `json:"userland,omitempty"`
	Metadata    cid.Cid  `json:"metadata,omitempty"`
	SubSkeleton Skeleton `json:"subSkeleton,omitempty"`
	IsFile      bool     `json:"isFile"`
}

type Skeleton map[string]SkeletonInfo

func LoadSkeleton(ctx context.Context, store FileGetter, id cid.Cid) (Skeleton, error) {
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
	return sk, base.DecodeCBOR(d, &sk)
}

func (s Skeleton) CBORFile() (fs.File, error) {
	buf, err := base.EncodeCBOR(s)
	if err != nil {
		return nil, err
	}

	return base.NewMemfileReader(base.SkeletonLinkName, buf), nil
	// return &base.memfile{
	// 	fi: &base.FSFileInfo{
	// 		name:  SkeletonLinkName,
	// 		size:  int64(buf.Len()),
	// 		mode:  0755,
	// 		mtime: Timestamp(),
	// 	},
	// 	buf: buf,
	// }, nil
}

func (s Skeleton) PathInfo(path base.Path) (SkeletonInfo, error) {
	head, tail := path.Shift()
	info, ok := s[head]
	if !ok {
		return info, base.ErrNotFound
	}

	if tail != nil {
		return info.SubSkeleton.PathInfo(tail)
	}
	return info, nil
}
