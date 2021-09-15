package mdstore_test

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/mdstore"
	mock "github.com/qri-io/wnfs-go/mdstore/mock"
)

func TestMerkleDagBlocks(t *testing.T) {
	ctx, mds := setupMDS(t)

	if _, err := mdstore.NewMerkleDagStore(ctx, nil); err == nil {
		t.Error("expected passing a nil blockstore to NewMerkleDagStore to error. got nil.")
	}

	if mds.Blockstore() == nil {
		t.Error("expected blockstore method to return a non-nil blockstore")
	}

	in := []byte("hello")
	id, err := mds.PutBlock(in)
	if err != nil {
		t.Fatal(err)
	}

	got, err := mds.GetBlock(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(in, got); diff != "" {
		t.Errorf("result mismatch: (-want +got):\n%s", diff)
	}
}

func TestMerkleDagNodes(t *testing.T) {
	ctx, mds := setupMDS(t)

	id, err := mds.PutBlock([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	node, err := mds.PutNode(mdstore.NewLinks(mdstore.Link{Name: "greeting", Cid: id}))
	if err != nil {
		t.Fatal(err)
	}

	got, err := mds.GetNode(ctx, node.Cid)
	if err != nil {
		t.Fatal(err)
	}

	if !got.Cid().Equals(node.Cid) {
		t.Errorf("cid mismatch.\nwant: %s\ngot:  %s", node.Cid, got.Cid())
	}
}

func TestMerkleDagFiles(t *testing.T) {
	ctx, mds := setupMDS(t)
	content := "y" + strings.Repeat("o", 1024*256*2) // yooooooooooo

	res, err := mds.PutFile(base.NewMemfileReader("yo.txt", strings.NewReader(content)))
	if err != nil {
		t.Fatal(err)
	}

	rdr, err := mds.GetFile(ctx, res.Cid)
	if err != nil {
		t.Fatal(err)
	}

	got, err := ioutil.ReadAll(rdr)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff([]byte(content), got); diff != "" {
		t.Errorf("content mismatch. (-want +got):\n%s", diff)
	}
}

func setupMDS(t *testing.T) (context.Context, mdstore.MerkleDagStore) {
	t.Helper()

	ctx := context.Background()

	mds, err := mdstore.NewMerkleDagStore(ctx, mock.NewOfflineMemBlockservice())
	if err != nil {
		t.Fatal(err)
	}

	return ctx, mds
}
