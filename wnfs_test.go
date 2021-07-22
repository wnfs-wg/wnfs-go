package wnfs

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	mockipfs "github.com/qri-io/wnfs-go/ipfs/mock"
	"github.com/qri-io/wnfs-go/mdstore"
)

func TestWNFS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("writes_files", func(t *testing.T) {
		store, err := mockipfs.MockMerkleDagStore(ctx)
		if err != nil {
			t.Fatal(err)
		}

		fsys, err := NewEmptyFS(ctx, store)
		if err != nil {
			t.Fatal(err)
		}

		pathStr := "public/foo/hello.txt"
		fileContents := []byte("hello!")
		f := NewMemfileBytes("hello.txt", fileContents)

		if err := fsys.Write(pathStr, f, MutationOptions{Commit: true}); err != nil {
			t.Error(err)
		}

		t.Logf("wnfs root CID: %s", fsys.(mdstore.DagNode).Cid())

		gotFileContents, err := fsys.Cat(pathStr)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(fileContents, gotFileContents); diff != "" {
			t.Errorf("result mismatch. (-want +got):\n%s", diff)
		}
	})
}

func TestPath(t *testing.T) {
	p, err := NewPath("public/baz.txt")
	if err != nil {
		t.Fatal(err)
	}

	got, tail := p.Shift()
	want := "public"
	if want != got {
		t.Errorf("result mismatch. want: %q got: %q", want, got)
	}
	wantTail := Path{"baz.txt"}
	if diff := cmp.Diff(wantTail, tail); diff != "" {
		t.Errorf("result mismatch, (-want +got):\n%s", diff)
	}

	got, tail = tail.Shift()
	want = "baz.txt"
	if want != got {
		t.Errorf("result mismatch. want: %q got: %q", want, got)
	}
	if tail != nil {
		t.Errorf("expected tail to equal nil. got: %v", tail)
	}
}
