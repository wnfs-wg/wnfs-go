package private

import (
	"context"
	"os"
	"testing"

	golog "github.com/ipfs/go-log"
	base "github.com/qri-io/wnfs-go/base"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
	mdstoremock "github.com/qri-io/wnfs-go/mdstore/mock"
	ratchet "github.com/qri-io/wnfs-go/ratchet"
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
