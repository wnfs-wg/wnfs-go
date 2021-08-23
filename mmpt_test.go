package wnfs

import (
	"context"
	"testing"

	mockipfs "github.com/qri-io/wnfs-go/ipfs/mock"
	"github.com/qri-io/wnfs-go/mdstore"
)

func TestMMPT(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := mockipfs.MockMerkleDagStore(ctx)
	if err != nil {
		t.Fatal(err)
	}

	res, err := store.PutFile(NewMemfileBytes("hello.txt", []byte("hello world!")))
	if err != nil {
		t.Fatal(err)
	}

	tree := NewMMPT(store, mdstore.NewLinks())
	if err := tree.Add("0abc", res.Cid); err != nil {
		t.Fatal(err)
	}
	if err := tree.Add("0abd", res.Cid); err != nil {
		t.Fatal(err)
	}
	result, err := tree.Put()
	if err != nil {
		t.Fatal(err)
	}

	tree, err = LoadMMPT(store, result.Cid)
	if err != nil {
		t.Fatal(err)
	}

	ms, err := tree.Members()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(ms)
}
