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
