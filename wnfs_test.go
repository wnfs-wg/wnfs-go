package wnfs

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	cmp "github.com/google/go-cmp/cmp"
	golog "github.com/ipfs/go-log"
	base "github.com/qri-io/wnfs-go/base"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
	mdstoremock "github.com/qri-io/wnfs-go/mdstore/mock"
	"github.com/qri-io/wnfs-go/ratchet"
)

var testRootKey Key = [32]byte{
	1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	1, 2,
}

func init() {
	if lvl := os.Getenv("WNFS_LOGGING"); lvl != "" {
		golog.SetLogLevel("wnfs", lvl)
	}
}

func TestPublicWNFS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("writes_files", func(t *testing.T) {
		store := newMemTestStore(ctx, t)
		rs := ratchet.NewMemStore(ctx)

		fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
		if err != nil {
			t.Fatal(err)
		}

		pathStr := "public/foo/hello.txt"
		fileContents := []byte("hello!")
		f := base.NewMemfileBytes("hello.txt", fileContents)

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

		ents, err := fsys.Ls("public/foo")
		if err != nil {
			t.Error(err)
		}
		if len(ents) != 1 {
			t.Errorf("expected 1 entries. got: %d", len(ents))
		}

		if err := fsys.Rm(pathStr, MutationOptions{Commit: true}); err != nil {
			t.Error(err)
		}

		_, err = fsys.Cat(pathStr)
		if !errors.Is(err, base.ErrNotFound) {
			t.Errorf("expected calling cat on removed path to return wrap of base.ErrNotFound. got: %s", err)
		}

		if err := fsys.Mkdir("public/bar"); err != nil {
			t.Error(err)
		}

		ents, err = fsys.Ls("public/foo")
		if err != nil {
			t.Error(err)
		}
		if len(ents) != 0 {
			t.Errorf("expected no entries. got: %d", len(ents))
		}

		ents, err = fsys.Ls("public")
		if err != nil {
			t.Error(err)
		}
		if len(ents) != 2 {
			t.Errorf("expected 2 entries. got: %d", len(ents))
		}

		dfs := os.DirFS("./testdata")
		if err := fsys.Cp("public/cats", "cats", dfs, MutationOptions{Commit: true}); err != nil {
			t.Error(err)
		}

		ents, err = fsys.Ls("public/cats")
		if err != nil {
			t.Error(err)
		}
		if len(ents) != 2 {
			t.Errorf("expected 2 entries. got: %d", len(ents))
		}
	})
}

func BenchmarkPublicCat10MbFile(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	textFile := base.NewMemfileBytes("bench.txt", data)
	fsys.Write("public/bench.txt", textFile, MutationOptions{
		Commit: true,
	})
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		if _, err := fsys.Cat("public/bench.txt"); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkPublicWrite10MbFile(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	textFile := base.NewMemfileBytes("bench.txt", data)
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		fsys.Write("public/bench.txt", textFile, MutationOptions{
			Commit: true,
		})
	}
}

func BenchmarkPublicCat10MbFileSubdir(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	textFile := base.NewMemfileBytes("bench.txt", data)
	fsys.Write("public/subdir/bench.txt", textFile, MutationOptions{
		Commit: true,
	})
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		if _, err := fsys.Cat("public/subdir/bench.txt"); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkPublicWrite10MbFileSubdir(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	textFile := base.NewMemfileBytes("bench.txt", data)
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		fsys.Write("public/subdir/bench.txt", textFile, MutationOptions{
			Commit: true,
		})
	}
}

func BenchmarkPublicCp10DirectoriesWithOne10MbFileEach(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	dir, err := ioutil.TempDir("", "bench_10_single_file_directories")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	for i := 0; i < 10; i++ {
		path := filepath.Join(dir, "copy_me", fmt.Sprintf("dir_%d", i))
		os.MkdirAll(path, 0755)
		path = filepath.Join(path, "bench.txt")

		data := make([]byte, 1024*10)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		ioutil.WriteFile(path, data, os.ModePerm)
	}

	dirFS := os.DirFS(dir)
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		fsys.Cp("public/copy_me", "copy_me", dirFS, MutationOptions{
			Commit: true,
		})
	}

	if _, err := fsys.Open("public/copy_me/dir_0/bench.txt"); err != nil {
		t.Fatal(err)
	}
}

func TestWNFSPrivate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	pathStr := "private/foo/hello.txt"
	fileContents := []byte("hello!")
	f := base.NewMemfileBytes("hello.txt", fileContents)

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

	ents, err := fsys.Ls("private/foo")
	if err != nil {
		t.Error(err)
	}
	if len(ents) != 1 {
		t.Errorf("expected 1 entries. got: %d", len(ents))
	}

	if err := fsys.Rm(pathStr, MutationOptions{Commit: true}); err != nil {
		t.Error(err)
	}

	_, err = fsys.Cat(pathStr)
	if !errors.Is(err, base.ErrNotFound) {
		t.Errorf("expected calling cat on removed path to return wrap of base.ErrNotFound. got: %s", err)
	}

	if err := fsys.Mkdir("private/bar"); err != nil {
		t.Error(err)
	}

	ents, err = fsys.Ls("private/foo")
	if err != nil {
		t.Error(err)
	}
	if len(ents) != 0 {
		t.Errorf("expected no entries. got: %d", len(ents))
	}

	ents, err = fsys.Ls("private")
	if err != nil {
		t.Error(err)
	}
	if len(ents) != 2 {
		t.Errorf("expected 2 entries. got: %d", len(ents))
	}

	dfs := os.DirFS("./testdata")
	if err := fsys.Cp("private/cats", "cats", dfs, MutationOptions{Commit: true}); err != nil {
		t.Error(err)
	}

	ents, err = fsys.Ls("private/cats")
	if err != nil {
		t.Error(err)
	}
	if len(ents) != 2 {
		t.Errorf("expected 2 entries. got: %d", len(ents))
	}

	// close context
	cancel()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	key := fsys.RootKey()
	rootCid := fsys.Cid()
	pn, err := fsys.PrivateName()
	if err != nil {
		t.Fatal(err)
	}

	fsys, err = FromCID(ctx2, store, rs, rootCid, key, pn)
	if err != nil {
		t.Fatal(err)
	}

	ents, err = fsys.Ls("private/cats")
	if err != nil {
		t.Error(err)
	}
	if len(ents) != 2 {
		t.Errorf("expected 2 entries. got: %d", len(ents))
	}
}

func BenchmarkPrivateCat10MbFile(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	textFile := base.NewMemfileBytes("bench.txt", data)
	fsys.Write("private/bench.txt", textFile, MutationOptions{
		Commit: true,
	})
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		if _, err := fsys.Cat("private/bench.txt"); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkPrivateWrite10MbFile(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	textFile := base.NewMemfileBytes("bench.txt", data)
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		fsys.Write("private/bench.txt", textFile, MutationOptions{
			Commit: true,
		})
	}
}

func BenchmarkPrivateCat10MbFileSubdir(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	textFile := base.NewMemfileBytes("bench.txt", data)
	fsys.Write("private/subdir/bench.txt", textFile, MutationOptions{
		Commit: true,
	})
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		if _, err := fsys.Cat("private/subdir/bench.txt"); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkPrivateWrite10MbFileSubdir(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	textFile := base.NewMemfileBytes("bench.txt", data)
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		fsys.Write("private/subdir/bench.txt", textFile, MutationOptions{
			Commit: true,
		})
	}
}

func BenchmarkPrivateCp10DirectoriesWithOne10MbFileEach(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := ratchet.NewMemStore(ctx)
	store, cleanup := newFileTestStore(ctx, t)
	defer cleanup()
	fsys, err := NewEmptyFS(ctx, store, rs, testRootKey)
	if err != nil {
		t.Fatal(err)
	}

	dir, err := ioutil.TempDir("", "bench_10_single_file_directories")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	for i := 0; i < 10; i++ {
		path := filepath.Join(dir, "copy_me", fmt.Sprintf("dir_%d", i))
		os.MkdirAll(path, 0755)
		path = filepath.Join(path, "bench.txt")

		data := make([]byte, 1024*10)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		ioutil.WriteFile(path, data, os.ModePerm)
	}

	dirFS := os.DirFS(dir)
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		fsys.Cp("private/copy_me", "copy_me", dirFS, MutationOptions{
			Commit: true,
		})
	}

	if _, err := fsys.Open("private/copy_me/dir_0/bench.txt"); err != nil {
		t.Fatal(err)
	}
}

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

func newMemTestPrivateStore(ctx context.Context, f fataler) mdstore.PrivateStore {
	f.Helper()
	rs := ratchet.NewMemStore(ctx)
	store, err := mdstore.NewPrivateStore(ctx, mdstoremock.NewOfflineMemBlockservice(), rs)
	if err != nil {
		f.Fatal(err)
	}
	return store
}

func newFileTestStore(ctx context.Context, f fataler) (st mdstore.MerkleDagStore, cleanup func()) {
	f.Helper()
	bserv, cleanup, err := mdstoremock.NewOfflineFileBlockservice(f.Name())
	if err != nil {
		f.Fatal(err)
	}

	store, err := mdstore.NewMerkleDagStore(ctx, bserv)
	if err != nil {
		f.Fatal(err)
	}

	return store, cleanup
}
