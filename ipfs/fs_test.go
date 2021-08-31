package ipfs_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	files "github.com/ipfs/go-ipfs-files"
	caopts "github.com/ipfs/interface-go-ipfs-core/options"
	corepath "github.com/ipfs/interface-go-ipfs-core/path"
	base "github.com/qri-io/wnfs-go/base"
	mockipfs "github.com/qri-io/wnfs-go/ipfs/mock"
)

func BenchmarkIPFSCat10MbFile(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := mockipfs.MockMerkleDagStore(ctx)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	textFile := base.NewMemfileBytes("bench.txt", data)
	res, err := store.PutFile(textFile)
	if err != nil {
		t.Fatal(err)
	}
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		f, err := store.GetFile(res.Cid)
		if err != nil {
			t.Fatal(err)
		}
		ioutil.ReadAll(f)
	}
}

func BenchmarkIPFSWrite10MbFile(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := mockipfs.MockMerkleDagStore(ctx)
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
		store.PutFile(textFile)
	}
}

func BenchmarkIPFSCat10MbFileSubdir(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, capi, err := mockipfs.MakeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	dir := files.NewMapDirectory(map[string]files.Node{
		"public": files.NewMapDirectory(map[string]files.Node{
			"subdir": files.NewMapDirectory(map[string]files.Node{
				"bench.txt": files.NewBytesFile(data),
			}),
		}),
	})

	path, err := capi.Unixfs().Add(ctx, dir, caopts.Unixfs.CidVersion(1))
	if err != nil {
		t.Fatal(err)
	}

	getPath := corepath.New(fmt.Sprintf("%s/public/subdir/bench.txt", path.Root()))
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		f, err := capi.Unixfs().Get(ctx, getPath)
		if err != nil {
			t.Fatal(err)
		}
		ioutil.ReadAll(f.(io.Reader))
	}
}

func BenchmarkIPFSWrite10MbFileSubdir(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, capi, err := mockipfs.MakeAPI(ctx)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 1024*10)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	dir := files.NewMapDirectory(map[string]files.Node{
		"public": files.NewMapDirectory(map[string]files.Node{
			"subdir": files.NewMapDirectory(map[string]files.Node{
				"bench.txt": files.NewBytesFile(data),
			}),
		}),
	})
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		_, err := capi.Unixfs().Add(ctx, dir, caopts.Unixfs.CidVersion(1))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkIPFSCp10DirectoriesWithOne10MbFileEach(t *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, capi, err := mockipfs.MakeAPI(ctx)
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

	st, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	serialDir, err := files.NewSerialFile(dir, false, st)
	if err != nil {
		t.Fatal(err)
	}
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		if _, err := capi.Unixfs().Add(ctx, serialDir, caopts.Unixfs.CidVersion(1)); err != nil {
			t.Fatal(err)
		}
	}

	// if _, err := capi.Unixfs().Open("public/copy_me/dir_0/bench.txt"); err != nil {
	// 	t.Fatal(err)
	// }
}
