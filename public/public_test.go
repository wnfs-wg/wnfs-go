package public

import (
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"testing"

	cmp "github.com/google/go-cmp/cmp"
	cmpopts "github.com/google/go-cmp/cmp/cmpopts"
	cid "github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log"
	base "github.com/qri-io/wnfs-go/base"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
	mdstoremock "github.com/qri-io/wnfs-go/mdstore/mock"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func init() {
	if lvl := os.Getenv("WNFS_LOGGING"); lvl != "" {
		golog.SetLogLevel("wnfs", lvl)
	}
}

func TestFileMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemTestStore(ctx, t)
	fs := mdfs{ctx: ctx, ds: store}
	expect := map[string]interface{}{
		"foo": "bar",
	}

	root := NewEmptyTree(fs, "root")
	root.SetMeta(expect)

	res, err := root.Put()
	require.Nil(t, err)

	root, err = LoadTree(ctx, fs, "root", res.CID())
	require.Nil(t, err)

	md, err := root.Meta()
	require.Nil(t, err)

	got, err := md.Data()
	require.Nil(t, err)

	assert.Equal(t, expect, got)
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

func TestHistory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newMemTestStore(ctx, t)
	fs := mdfs{ctx: ctx, ds: store}

	tree := NewEmptyTree(fs, "a")
	_, err := tree.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
	require.Nil(t, err)
	_, err = tree.Add(base.MustPath("salut.txt"), base.NewMemfileBytes("hello.txt", []byte("salut!")))
	require.Nil(t, err)
	_, err = tree.Add(base.MustPath("salut.txt"), base.NewMemfileBytes("hello.txt", []byte("salut 2!")))
	require.Nil(t, err)
	_, err = tree.Add(base.MustPath("dir/goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
	require.Nil(t, err)
	_, err = tree.Add(base.MustPath("dir/goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye 2!")))
	require.Nil(t, err)
	_, err = tree.Add(base.MustPath("dir/bonjour.txt"), base.NewMemfileBytes("bonjour.txt", []byte("bonjour!")))
	require.Nil(t, err)

	hist := mustHistCids(t, tree, base.Path{})
	assert.Equal(t, 6, len(hist))

	hist = mustHistCids(t, tree, base.MustPath("salut.txt"))
	assert.Equal(t, 2, len(hist))

	hist = mustHistCids(t, tree, base.Path{"dir"})
	assert.Equal(t, 3, len(hist))

	hist = mustHistCids(t, tree, base.Path{"dir", "goodbye.txt"})
	assert.Equal(t, 2, len(hist))

	hist = mustHistCids(t, tree, base.Path{"dir", "bonjour.txt"})
	assert.Equal(t, 1, len(hist))
}

func TestDataFileCoding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemTestStore(ctx, t)
	fs := mdfs{ctx: ctx, ds: store}

	data := []interface{}{"oh", "hai"}
	df := NewDataFile(fs, "data_file", data)
	_, err := df.Put()
	require.Nil(t, err)

	got, err := LoadDataFile(ctx, fs, df.Name(), df.Cid())
	require.Nil(t, err)

	assert.Equal(t, df, got)
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

func mustHistCids(t *testing.T, tree *PublicTree, path base.Path) []cid.Cid {
	t.Helper()
	n, err := tree.Get(path)
	require.Nil(t, err)
	log, err := n.(base.Node).History(context.Background(), -1)
	require.Nil(t, err)
	ids := make([]cid.Cid, len(log))
	for i, l := range log {
		ids[i] = l.Cid
	}
	return ids
}

func mustDirChildren(t *testing.T, dir *PublicTree, ch []string) {
	t.Helper()
	ents, err := dir.ReadDir(-1)
	require.Nil(t, err)

	got := make([]string, 0, len(ents))
	for _, ch := range ents {
		got = append(got, ch.Name())
	}

	assert.Equal(t, ch, got)
}

func mustFileContents(t *testing.T, dir *PublicTree, path, content string) {
	t.Helper()
	f, err := dir.Get(base.MustPath(path))
	require.Nil(t, err)
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	require.Nil(t, err)

	assert.Equal(t, content, string(data))
}
