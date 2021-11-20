package base

import (
	"context"
	"errors"
	"io"
	"io/fs"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

var ErrNotFound = errors.New("not found")

type BareFile struct {
	store mdstore.MerkleDagStore

	name string
	id   cid.Cid
	size int64

	content io.Reader
}

var (
	_ fs.File = (*BareFile)(nil)
)

func NewBareFile(store mdstore.MerkleDagStore, name string, r io.Reader) *BareFile {
	return &BareFile{
		store:   store,
		name:    name,
		content: r,
	}
}

func BareFileFromCID(ctx context.Context, store mdstore.MerkleDagStore, id cid.Cid) (*BareFile, error) {
	fs, err := store.GetFile(ctx, id)
	if err != nil {
		return nil, err
	}

	return &BareFile{
		store:   store,
		content: fs,
	}, nil
}

func (f *BareFile) Name() string         { return f.name }
func (f *BareFile) Size() int64          { return f.size }
func (f *BareFile) Cid() cid.Cid         { return f.id }
func (f *BareFile) Links() mdstore.Links { return mdstore.NewLinks() }

func (f *BareFile) Write() (PutResult, error) {
	return nil, errors.New("unifnished: BareFile.Write")
}

func (f *BareFile) Stat() (fs.FileInfo, error) {
	return FSFileInfo{
		name: f.name,
		size: f.size,
		sys:  f.store,
	}, nil
}

func (f *BareFile) Read(p []byte) (n int, err error) {
	return f.content.Read(p)
}

func (f *BareFile) Close() error {
	if closer, ok := f.content.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type BareTree struct {
	store mdstore.MerkleDagStore

	name string
	id   cid.Cid
	size int64

	links mdstore.Links
}

var (
	_ fs.File        = (*BareTree)(nil)
	_ fs.ReadDirFile = (*BareTree)(nil)
)

func (t *BareTree) Name() string         { return t.name }
func (t *BareTree) Size() int64          { return t.size }
func (t *BareTree) Cid() cid.Cid         { return t.id }
func (t *BareTree) Links() mdstore.Links { return t.links }

func (t *BareTree) Read(p []byte) (n int, err error) {
	return -1, errors.New("cannot read bytes from directory")
}
func (t *BareTree) Close() error {
	return nil
}
func (t *BareTree) Stat() (fs.FileInfo, error) {
	return &FSFileInfo{
		name: t.name,
		size: t.size,
		// TODO(b5):
		// mtime: time.Time,
		mode: fs.ModeDir,
		sys:  t.store,
	}, nil
}

func (t *BareTree) ReadDir(n int) ([]fs.DirEntry, error) {
	if n < 0 {
		n = t.links.Len()
	}

	entries := make([]fs.DirEntry, 0, n)
	for i, link := range t.links.SortedSlice() {
		entries = append(entries, FSDirEntry{
			name:   link.Name,
			isFile: link.IsFile,
		})

		if i == n {
			break
		}
	}

	return entries, nil
}
