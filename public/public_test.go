package public

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"testing"

	cmp "github.com/google/go-cmp/cmp"
	cmpopts "github.com/google/go-cmp/cmp/cmpopts"
	cid "github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log"
	base "github.com/qri-io/wnfs-go/base"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
	mdstoremock "github.com/qri-io/wnfs-go/mdstore/mock"
)

func init() {
	if lvl := os.Getenv("WNFS_LOGGING"); lvl != "" {
		golog.SetLogLevel("wnfs", lvl)
	}
}

func TestTreeSkeleton(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemTestStore(ctx, t)
	fs := mdfs{ctx: ctx, ds: store}

	root := NewEmptyTree(fs, "")
	root.Add(base.MustPath("foo/bar/baz/hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
	root.Add(base.MustPath("bar/baz/goodbye"), base.NewMemfileBytes("goodbye", []byte(`goodbye`)))
	root.Add(base.MustPath("some.json"), base.NewMemfileBytes("some.json", []byte(`{"oh":"hai}`)))

	expect := base.Skeleton{
		"bar": base.SkeletonInfo{
			SubSkeleton: base.Skeleton{
				"baz": base.SkeletonInfo{
					SubSkeleton: base.Skeleton{
						"goodbye": base.SkeletonInfo{IsFile: true},
					},
				},
			},
		},
		"foo": base.SkeletonInfo{
			SubSkeleton: base.Skeleton{
				"bar": base.SkeletonInfo{
					SubSkeleton: base.Skeleton{
						"baz": base.SkeletonInfo{
							SubSkeleton: base.Skeleton{
								"hello.txt": base.SkeletonInfo{IsFile: true},
							},
						},
					},
				},
			},
		},
		"some.json": base.SkeletonInfo{IsFile: true},
	}

	got, err := root.Skeleton()
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(expect, got, cmpopts.IgnoreTypes(cid.Cid{})); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
}

type mdfs struct {
	ctx context.Context
	ds  mdstore.MerkleDagStore
}

var _ base.MerkleDagFS = (*mdfs)(nil)

func (fs mdfs) Open(path string) (fs.File, error) { return nil, fmt.Errorf("shim MDFS cannot open") }
func (fs mdfs) Context() context.Context          { return fs.ctx }
func (fs mdfs) DagStore() mdstore.MerkleDagStore  { return fs.ds }

type fataler interface {
	Name() string
	Helper()
	Fatal(args ...interface{})
}

func newMemTestStore(ctx context.Context, f fataler) mdstore.MerkleDagStore {
	f.Helper()
	store, err := mdstore.NewMerkleDagStore(ctx, mdstoremock.NewOfflineMemBlockservice())
	if err != nil {
		f.Fatal(err)
	}
	return store
}
