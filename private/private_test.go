package private

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	cid "github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log"
	base "github.com/qri-io/wnfs-go/base"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
	mdstoremock "github.com/qri-io/wnfs-go/mdstore/mock"
	ratchet "github.com/qri-io/wnfs-go/ratchet"
	"github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func init() {
	if lvl := os.Getenv("WNFS_LOGGING"); lvl != "" {
		golog.SetLogLevel("wnfs", lvl)
	}
}

func TestHistory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemTestPrivateStore(ctx, t)

	root, err := NewEmptyRoot(ctx, store, "private", testRootKey)
	require.Nil(t, err)

	_, err = root.Add(base.MustPath("hi.txt"), base.NewMemfileBytes("hi.txt", []byte("oh hello")))
	require.Nil(t, err)

	_, err = root.Put()
	require.Nil(t, err)

	_, err = root.Add(base.MustPath("hi.txt"), base.NewMemfileBytes("hi.txt", []byte("oh hello 2")))
	require.Nil(t, err)

	_, err = root.Put()
	require.Nil(t, err)

	hist, err := root.History(base.MustPath("hi.txt"), -1)
	require.Nil(t, err)

	t.Logf("%#v", hist)
}

func TestBasicTreeMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newMemTestPrivateStore(ctx, t)

	t.Run("no_common_history", func(t *testing.T) {
		a, err := NewEmptyRoot(ctx, store, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		b, err := NewEmptyRoot(ctx, store, "", testRootKey)
		require.Nil(t, err)
		// write to get an empty root
		_, err = b.Put()
		require.Nil(t, err)

		_, err = Merge(a, b)
		assert.ErrorIs(t, err, base.ErrNoCommonHistory)
		// require.Nil(t, err)
		// assert.Equal(t, base.MT, res.Type)
		err = a.Close()
		require.Nil(t, err)

		aKey := a.Key()
		aHamt := a.hamtRootCID
		pn, err := a.PrivateName()
		require.Nil(t, err)

		_, err = LoadRoot(ctx, a.store, "", *aHamt, aKey, pn)
		require.Nil(t, err)
	})

	t.Run("fast_forward", func(t *testing.T) {
		// local node is behind, fast-forward
		a, err := NewEmptyRoot(ctx, store, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		b, err := LoadRoot(ctx, a.store, a.name, a.Cid(), a.Key(), pn)
		require.Nil(t, err)
		_, err = b.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTFastForward, res.Type)
	})

	t.Run("local_ahead", func(t *testing.T) {
		a, err := NewEmptyRoot(ctx, store, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		b, err := LoadRoot(ctx, a.store, a.name, a.Cid(), a.Key(), pn)
		require.Nil(t, err)

		// local node is ahead, no-op for local merge
		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTLocalAhead, res.Type)

		err = a.Close()
		require.Nil(t, err)
	})
}

func TestTreeMergeCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newMemTestPrivateStore(ctx, t)

	t.Run("no_conflict_merge", func(t *testing.T) {
		a, err := NewEmptyRoot(ctx, store, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		b, err := LoadRoot(ctx, a.store, a.name, a.Cid(), a.Key(), pn)
		require.Nil(t, err)

		_, err = a.Add(base.MustPath("bonjour.txt"), base.NewMemfileBytes("bonjour.txt", []byte("bonjour!")))
		require.Nil(t, err)

		// local node is ahead, no-op for local merge
		_, err = b.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)

		key := &Key{}
		err = key.Decode(res.Key)
		require.Nil(t, err)
		a, err = LoadRoot(ctx, a.store, res.Name, *res.HamtRoot, *key, Name(res.PrivateName))
		require.Nil(t, err)

		// TODO(b5): not doing merge commit fields yet
		// assert.NotNil(t, a.merge)
		mustDirChildren(t, a, []string{
			"bonjour.txt",
			"goodbye.txt",
			"hello.txt",
		})
		mustFileContents(t, a, "goodbye.txt", "goodbye!")
		mustFileContents(t, a, "hello.txt", "hello!")
	})

	t.Run("remote_overwrites_local_file", func(t *testing.T) {
		a, err := NewEmptyRoot(ctx, store, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		b, err := LoadRoot(ctx, a.store, a.name, a.Cid(), a.Key(), pn)
		require.Nil(t, err)

		_, err = b.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello **2**, written on remote")))
		require.Nil(t, err)

		// add to a to diverge histories
		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)

		key := &Key{}
		err = key.Decode(res.Key)
		require.Nil(t, err)
		a, err = LoadRoot(ctx, a.store, res.Name, *res.HamtRoot, *key, Name(res.PrivateName))
		require.Nil(t, err)

		mustDirChildren(t, a, []string{
			"goodbye.txt",
			"hello.txt",
		})
		mustFileContents(t, a, "hello.txt", "hello **2**, written on remote")
	})

	t.Run("local_overwrites_remote_file", func(t *testing.T) {
		a, err := NewEmptyRoot(ctx, store, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		b, err := LoadRoot(ctx, a.store, a.name, a.Cid(), a.Key(), pn)
		require.Nil(t, err)
		_, err = b.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello **2** (remote)")))
		require.Nil(t, err)

		// a has more commits, should win
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello **2**")))
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello **3**")))
		require.Nil(t, err)

		res, err := Merge(a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)

		key := &Key{}
		err = key.Decode(res.Key)
		require.Nil(t, err)
		a, err = LoadRoot(ctx, a.store, res.Name, *res.HamtRoot, *key, Name(res.PrivateName))
		require.Nil(t, err)
		mustDirChildren(t, a, []string{
			"hello.txt",
		})
		mustFileContents(t, a, "hello.txt", "hello **3**")
	})

	t.Run("remote_deletes_local_file", func(t *testing.T) {
		t.Logf(`
	TODO (b5): This implementation makes it difficult to delete files. The file here
	is restored upon merge, and would need to be removed in *both* trees to be
	removed entirely. should consult spec for correctness`[1:])
		a, err := NewEmptyRoot(ctx, store, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		b, err := LoadRoot(ctx, a.store, a.name, a.Cid(), a.Key(), pn)
		require.Nil(t, err)
		_, err = b.Rm(base.MustPath("hello.txt"))
		require.Nil(t, err)

		// add to a to diverge histories
		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)

		key := &Key{}
		err = key.Decode(res.Key)
		require.Nil(t, err)
		a, err = LoadRoot(ctx, a.store, res.Name, *res.HamtRoot, *key, Name(res.PrivateName))
		require.Nil(t, err)
		mustDirChildren(t, a, []string{
			"goodbye.txt",
			"hello.txt",
		})
	})

	// 	t.Run("local_deletes_file", func(t *testing.T) {
	// 		t.Logf(`
	// TODO (b5): This implementation makes it difficult to delete files. The file here
	// is restored upon merge, and would need to be removed in *both* trees to be
	// removed entirely. should consult spec for correctness`[1:])
	// 		a := NewEmptyTree(fs, "")
	// 		_, err := a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
	// 		require.Nil(t, err)

	// 		b, err := LoadTreeFromCID(a.fs, a.Name(), a.Cid())
	// 		require.Nil(t, err)
	// 		_, err = a.Rm(base.MustPath("hello.txt"))
	// 		require.Nil(t, err)

	// 		// add to a to diverge histories
	// 		_, err = b.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
	// 		require.Nil(t, err)

	// 		res, err := Merge(a, b)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, base.MTMergeCommit, res.Type)
	// 		a, err = LoadTreeFromCID(fs, "", res.Cid)
	// 		require.Nil(t, err)
	// 		mustDirChildren(t, a, []string{
	// 			"goodbye.txt",
	// 			"hello.txt",
	// 		})
	// 	})

	// 	t.Run("remote_deletes_local_dir", func(t *testing.T) {
	// 		t.Skip("TODO(b5)")
	// 	})
	// 	t.Run("local_deletes_remote_dir", func(t *testing.T) {
	// 		t.Skip("TODO(b5)")
	// 	})

	// 	t.Run("remote_overwrites_local_file_with_directory", func(t *testing.T) {
	// 		a := NewEmptyTree(fs, "")
	// 		_, err := a.Add(base.MustPath("hello"), base.NewMemfileBytes("hello", []byte("hello!")))
	// 		require.Nil(t, err)

	// 		b, err := LoadTreeFromCID(a.fs, a.Name(), a.Cid())
	// 		require.Nil(t, err)
	// 		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
	// 		require.Nil(t, err)

	// 		_, err = b.Rm(base.MustPath("hello"))
	// 		require.Nil(t, err)

	// 		_, err = b.Mkdir(base.MustPath("hello"))
	// 		require.Nil(t, err)

	// 		res, err := Merge(a, b)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, base.MTMergeCommit, res.Type)
	// 		a, err = LoadTreeFromCID(fs, "", res.Cid)
	// 		require.Nil(t, err)
	// 		mustDirChildren(t, a, []string{
	// 			"goodbye.txt",
	// 			"hello",
	// 		})
	// 	})
	// 	t.Run("local_overwrites_remote_file_with_directory", func(t *testing.T) {
	// 		t.Skip("TODO(b5)")
	// 	})

	// 	t.Run("remote_overwrites_local_directory_with_file", func(t *testing.T) {
	// 		t.Skip("TODO(b5)")
	// 	})
	// 	t.Run("local_overwrites_remote_directory_with_file", func(t *testing.T) {
	// 		t.Skip("TODO(b5)")
	// 	})

	// 	t.Run("remote_delete_undeleted_by_local_edit", func(t *testing.T) {
	// 		t.Skip("TODO(b5)")
	// 	})
	// 	t.Run("local_delete_undeleted_by_remote_edit", func(t *testing.T) {
	// 		t.Skip("TODO(b5)")
	// 	})

	// 	t.Run("merge_remote_into_local_then_sync_local_to_remote", func(t *testing.T) {
	// 		t.Skip("TODO(b5)")
	// 	})
}

type fataler interface {
	Name() string
	Helper()
	Fatal(args ...interface{})
}

func newMemTestPrivateStore(ctx context.Context, f fataler) mdstore.PrivateStore {
	f.Helper()
	rs := ratchet.NewMemStore(ctx)
	store, err := mdstore.NewPrivateStore(ctx, mdstoremock.NewOfflineMemBlockservice(), rs)
	if err != nil {
		f.Fatal(err)
	}
	return store
}

func newMemTestStore(ctx context.Context, f fataler) mdstore.MerkleDagStore {
	f.Helper()
	store, err := mdstore.NewMerkleDagStore(ctx, mdstoremock.NewOfflineMemBlockservice())
	if err != nil {
		f.Fatal(err)
	}
	return store
}

func mustHistCids(t *testing.T, tree *Tree, path base.Path) []cid.Cid {
	t.Helper()
	log, err := tree.History(path, -1)
	require.Nil(t, err)
	ids := make([]cid.Cid, len(log))
	for i, l := range log {
		ids[i] = l.Cid
	}
	return ids
}

func mustDirChildren(t *testing.T, dir base.Tree, ch []string) {
	t.Helper()
	ents, err := dir.ReadDir(-1)
	require.Nil(t, err)

	got := make([]string, 0, len(ents))
	for _, ch := range ents {
		got = append(got, ch.Name())
	}

	assert.Equal(t, ch, got)
}

func mustFileContents(t *testing.T, dir base.Tree, path, content string) {
	t.Helper()
	f, err := dir.Get(base.MustPath(path))
	require.Nil(t, err)
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	require.Nil(t, err)

	assert.Equal(t, content, string(data))
}
