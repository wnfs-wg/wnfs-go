package public

import (
	"context"
	"testing"

	base "github.com/qri-io/wnfs-go/base"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestBasicTreeMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newMemTestStore(ctx, t)

	t.Run("no_common_history", func(t *testing.T) {
		a := NewEmptyTree(store, "")
		a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))

		b := NewEmptyTree(store, "")
		b.Add(base.MustPath("some_other_fs.txt"), base.NewMemfileBytes("some_other_fs.txt", []byte("some other filesystem")))

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		_, err = LoadTree(ctx, store, "", res.Cid)
		require.Nil(t, err)
	})

	t.Run("fast_forward", func(t *testing.T) {
		// local node is behind, fast-forward
		a := NewEmptyTree(store, "a")
		_, err := a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		b, err := LoadTree(ctx, a.store, a.Name(), a.Cid())
		require.Nil(t, err)
		_, err = b.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTFastForward, res.Type)
	})

	t.Run("local_ahead", func(t *testing.T) {
		a := NewEmptyTree(store, "")
		_, err := a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		b, err := LoadTree(ctx, a.store, a.Name(), a.Cid())
		require.Nil(t, err)

		// local node is ahead, no-op for local merge
		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTLocalAhead, res.Type)
	})
}

func TestTreeMergeCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newMemTestStore(ctx, t)

	t.Run("no_conflict_merge", func(t *testing.T) {
		a := NewEmptyTree(store, "")
		_, err := a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		b, err := LoadTree(ctx, a.store, a.Name(), a.Cid())
		require.Nil(t, err)

		_, err = a.Add(base.MustPath("bonjour.txt"), base.NewMemfileBytes("bonjour.txt", []byte("bonjour!")))
		require.Nil(t, err)

		// local node is ahead, no-op for local merge
		_, err = b.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)
		a, err = LoadTree(ctx, store, "", res.Cid)
		require.Nil(t, err)
		assert.NotNil(t, a.h.Merge)
		mustDirChildren(t, a, []string{
			"bonjour.txt",
			"goodbye.txt",
			"hello.txt",
		})
		mustFileContents(t, a, "goodbye.txt", "goodbye!")
		mustFileContents(t, a, "hello.txt", "hello!")
	})

	t.Run("remote_overwrites_local_file", func(t *testing.T) {
		a := NewEmptyTree(store, "")
		_, err := a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		b, err := LoadTree(ctx, a.store, a.Name(), a.Cid())
		require.Nil(t, err)

		_, err = b.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello **2**, written on remote")))
		require.Nil(t, err)

		// add to a to diverge histories
		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)
		a, err = LoadTree(ctx, store, "", res.Cid)
		require.Nil(t, err)
		mustDirChildren(t, a, []string{
			"goodbye.txt",
			"hello.txt",
		})
		mustFileContents(t, a, "hello.txt", "hello **2**, written on remote")
	})

	t.Run("local_overwrites_remote_file", func(t *testing.T) {
		a := NewEmptyTree(store, "")
		_, err := a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		b, err := LoadTree(ctx, a.store, a.Name(), a.Cid())
		require.Nil(t, err)
		_, err = b.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello **2** (remote)")))
		require.Nil(t, err)

		// a has more commits, should win
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello **2**")))
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello **3**")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)
		a, err = LoadTree(ctx, store, "", res.Cid)
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
		a := NewEmptyTree(store, "")
		_, err := a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		b, err := LoadTree(ctx, a.store, a.Name(), a.Cid())
		require.Nil(t, err)
		_, err = b.Rm(base.MustPath("hello.txt"))
		require.Nil(t, err)

		// add to a to diverge histories
		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)
		a, err = LoadTree(ctx, store, "", res.Cid)
		require.Nil(t, err)
		mustDirChildren(t, a, []string{
			"goodbye.txt",
			"hello.txt",
		})
	})

	t.Run("local_deletes_file", func(t *testing.T) {
		t.Logf(`
TODO (b5): This implementation makes it difficult to delete files. The file here
is restored upon merge, and would need to be removed in *both* trees to be 
removed entirely. should consult spec for correctness`[1:])
		a := NewEmptyTree(store, "")
		_, err := a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		b, err := LoadTree(ctx, a.store, a.Name(), a.Cid())
		require.Nil(t, err)
		_, err = a.Rm(base.MustPath("hello.txt"))
		require.Nil(t, err)

		// add to a to diverge histories
		_, err = b.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)
		a, err = LoadTree(ctx, store, "", res.Cid)
		require.Nil(t, err)
		mustDirChildren(t, a, []string{
			"goodbye.txt",
			"hello.txt",
		})
	})

	t.Run("remote_deletes_local_dir", func(t *testing.T) {
		t.Skip("TODO(b5)")
	})
	t.Run("local_deletes_remote_dir", func(t *testing.T) {
		t.Skip("TODO(b5)")
	})

	t.Run("remote_overwrites_local_file_with_directory", func(t *testing.T) {
		a := NewEmptyTree(store, "")
		_, err := a.Add(base.MustPath("hello"), base.NewMemfileBytes("hello", []byte("hello!")))
		require.Nil(t, err)

		b, err := LoadTree(ctx, a.store, a.Name(), a.Cid())
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		_, err = b.Rm(base.MustPath("hello"))
		require.Nil(t, err)

		_, err = b.Mkdir(base.MustPath("hello"))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)
		a, err = LoadTree(ctx, store, "", res.Cid)
		require.Nil(t, err)
		mustDirChildren(t, a, []string{
			"goodbye.txt",
			"hello",
		})
	})
	t.Run("local_overwrites_remote_file_with_directory", func(t *testing.T) {
		t.Skip("TODO(b5)")
	})

	t.Run("remote_overwrites_local_directory_with_file", func(t *testing.T) {
		t.Skip("TODO(b5)")
	})
	t.Run("local_overwrites_remote_directory_with_file", func(t *testing.T) {
		t.Skip("TODO(b5)")
	})

	t.Run("remote_delete_undeleted_by_local_edit", func(t *testing.T) {
		t.Skip("TODO(b5)")
	})
	t.Run("local_delete_undeleted_by_remote_edit", func(t *testing.T) {
		t.Skip("TODO(b5)")
	})

	t.Run("merge_remote_into_local_then_sync_local_to_remote", func(t *testing.T) {
		t.Skip("TODO(b5)")
	})
}
