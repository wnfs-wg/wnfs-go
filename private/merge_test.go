package private

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

	t.Run("no_common_history", func(t *testing.T) {
		aStore := newMemTestPrivateStore(ctx, t)
		bStore := newMemTestPrivateStore(ctx, t)

		a, err := NewEmptyRoot(ctx, aStore, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		b, err := NewEmptyRoot(ctx, bStore, "", testRootKey)
		require.Nil(t, err)
		// write to get an empty root
		_, err = b.Put()
		require.Nil(t, err)

		_, err = Merge(ctx, a, b)
		assert.ErrorIs(t, err, base.ErrNoCommonHistory)
		err = a.Close()
		require.Nil(t, err)

		aKey := a.Key()
		pn, err := a.PrivateName()
		require.Nil(t, err)

		_, err = LoadRoot(ctx, aStore, "", aKey, pn)
		require.Nil(t, err)
	})

	t.Run("fast_forward", func(t *testing.T) {
		aStore := newMemTestPrivateStore(ctx, t)
		a, err := NewEmptyRoot(ctx, aStore, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		bStore := copyStore(ctx, aStore, t)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		b, err := LoadRoot(ctx, bStore, a.name, a.Key(), pn)
		require.Nil(t, err)
		_, err = b.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		// local node is behind, fast-forward
		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTFastForward, res.Type)
	})

	t.Run("local_ahead", func(t *testing.T) {
		aStore := newMemTestPrivateStore(ctx, t)
		a, err := NewEmptyRoot(ctx, aStore, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		bStore := copyStore(ctx, aStore, t)
		b, err := LoadRoot(ctx, bStore, a.name, a.Key(), pn)
		require.Nil(t, err)

		// local node is ahead, no-op for local merge
		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTLocalAhead, res.Type)

		err = a.Close()
		require.Nil(t, err)
	})
}

func TestTreeMergeCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("no_conflict_merge", func(t *testing.T) {
		aStore := newMemTestPrivateStore(ctx, t)
		a, err := NewEmptyRoot(ctx, aStore, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		bStore := copyStore(ctx, aStore, t)
		b, err := LoadRoot(ctx, bStore, a.name, a.Key(), pn)
		require.Nil(t, err)

		_, err = a.Add(base.MustPath("bonjour.txt"), base.NewMemfileBytes("bonjour.txt", []byte("bonjour!")))
		require.Nil(t, err)

		// local node is ahead, no-op for local merge
		_, err = b.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)

		key := &Key{}
		err = key.Decode(res.Key)
		require.Nil(t, err)
		a, err = LoadRoot(ctx, aStore, res.Name, *key, Name(res.PrivateName))
		require.Nil(t, err)

		assertHasAllBlocks(t, aStore.Blockservice().Blockstore(), bStore.Blockservice().Blockstore())

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
		aStore := newMemTestPrivateStore(ctx, t)
		a, err := NewEmptyRoot(ctx, aStore, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		bStore := copyStore(ctx, aStore, t)
		b, err := LoadRoot(ctx, bStore, a.name, a.Key(), pn)
		require.Nil(t, err)

		_, err = b.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello **2**, written on remote")))
		require.Nil(t, err)

		// add to a to diverge histories
		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)

		key := &Key{}
		err = key.Decode(res.Key)
		require.Nil(t, err)
		a, err = LoadRoot(ctx, aStore, res.Name, *key, Name(res.PrivateName))
		require.Nil(t, err)

		mustDirChildren(t, a, []string{
			"goodbye.txt",
			"hello.txt",
		})
		mustFileContents(t, a, "hello.txt", "hello **2**, written on remote")
	})

	t.Run("local_overwrites_remote_file", func(t *testing.T) {
		aStore := newMemTestPrivateStore(ctx, t)
		a, err := NewEmptyRoot(ctx, aStore, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		bStore := copyStore(ctx, aStore, t)
		b, err := LoadRoot(ctx, bStore, a.name, a.Key(), pn)
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

		key := &Key{}
		err = key.Decode(res.Key)
		require.Nil(t, err)
		a, err = LoadRoot(ctx, aStore, res.Name, *key, Name(res.PrivateName))
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
		aStore := newMemTestPrivateStore(ctx, t)
		a, err := NewEmptyRoot(ctx, aStore, "", testRootKey)
		require.Nil(t, err)
		_, err = a.Add(base.MustPath("hello.txt"), base.NewMemfileBytes("hello.txt", []byte("hello!")))
		require.Nil(t, err)

		pn, err := a.PrivateName()
		require.Nil(t, err)
		bStore := copyStore(ctx, aStore, t)
		b, err := LoadRoot(ctx, bStore, a.name, a.Key(), pn)
		require.Nil(t, err)
		_, err = b.Rm(base.MustPath("hello.txt"))
		require.Nil(t, err)

		// add to a to diverge histories
		_, err = a.Add(base.MustPath("goodbye.txt"), base.NewMemfileBytes("goodbye.txt", []byte("goodbye!")))
		require.Nil(t, err)

		res, err := Merge(ctx, a, b)
		require.Nil(t, err)
		assert.Equal(t, base.MTMergeCommit, res.Type)

		key := &Key{}
		err = key.Decode(res.Key)
		require.Nil(t, err)
		a, err = LoadRoot(ctx, aStore, res.Name, *key, Name(res.PrivateName))
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

	// 		res, err := Merge(ctx, a, b)
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

	// 		res, err := Merge(ctx, a, b)
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
