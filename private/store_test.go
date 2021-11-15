package private

import (
	"bytes"
	"context"
	"io/ioutil"
	"sort"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	base "github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/mdstore"
	mdstoremock "github.com/qri-io/wnfs-go/mdstore/mock"
	ratchet "github.com/qri-io/wnfs-go/ratchet"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestCopyBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storeA := newMemTestPrivateStore(ctx, t)
	storeB := newMemTestPrivateStore(ctx, t)
	fileContents := []byte(bytes.Repeat([]byte("test"), 200000)) // large to make dag > 1 block

	res, err := storeA.PutEncryptedFile(base.NewMemfileBytes("", fileContents), testRootKey[:])
	require.Nil(t, err)

	err = CopyBlocks(ctx, res.Cid, storeA, storeB)
	require.Nil(t, err)

	data, err := storeB.GetEncryptedFile(res.Cid, testRootKey[:])
	require.Nil(t, err)

	got, err := ioutil.ReadAll(data)
	require.Nil(t, err)

	assert.Equal(t, fileContents, got)
}

func newMemTestPrivateStore(ctx context.Context, f fataler) Store {
	f.Helper()
	store, err := NewStore(ctx, mdstoremock.NewOfflineMemBlockservice(), ratchet.NewMemStore(ctx))
	if err != nil {
		f.Fatal(err)
	}
	return store
}

func copyStore(ctx context.Context, a Store, t *testing.T) Store {
	t.Helper()
	store, err := NewStore(ctx, mdstoremock.NewOfflineMemBlockservice(), ratchet.NewMemStore(ctx))
	require.Nil(t, err)

	err = store.HAMT().Merge(ctx, a.HAMT().root)
	require.Nil(t, err)

	rs := store.RatchetStore()
	err = a.RatchetStore().ForEach(ctx, func(name string, ratchet *ratchet.Spiral) error {
		_, err := rs.PutRatchet(ctx, name, ratchet)
		return err
	})
	require.Nil(t, err)

	abs := a.Blockservice().Blockstore()
	keys, err := abs.AllKeysChan(ctx)
	require.Nil(t, err)

	blks := make([]blocks.Block, 0)
	for key := range keys {
		blk, err := abs.Get(key)
		require.Nil(t, err)
		blks = append(blks, blk)
	}
	err = store.Blockservice().Blockstore().PutMany(blks)
	require.Nil(t, err)

	return store
}

func equalBlockstores(t *testing.T, a, b blockstore.Blockstore) {
	ctx := context.Background()
	aKeys, err := mdstore.AllKeys(ctx, a)
	require.Nil(t, err)

	bKeys, err := mdstore.AllKeys(ctx, b)
	require.Nil(t, err)

	astrs := cidsToStrings(aKeys)
	bstrs := cidsToStrings(bKeys)

	sort.Strings(astrs)
	sort.Strings(bstrs)

	assert.Equal(t, astrs, bstrs)
	t.Log("a keys:")
	for _, l := range astrs {
		t.Log(l)
	}

	t.Log("b keys:")
	for _, l := range bstrs {
		t.Log(l)
	}
}

func assertHasAllBlocks(t *testing.T, a, b blockstore.Blockstore) {
	ctx := context.Background()

	keys, err := b.AllKeysChan(ctx)
	require.Nil(t, err)

	missing := []cid.Cid{}
	for key := range keys {
		has, err := a.Has(key)
		require.Nil(t, err)
		if !has {
			missing = append(missing, key)
		}
	}

	if len(missing) > 0 {
		t.Errorf("%d blocks are missing:", len(missing))
		for _, s := range cidsToStrings(missing) {
			t.Logf(s)
		}
	}
}

func cidsToStrings(ids []cid.Cid) []string {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	return strs
}
