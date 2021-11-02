package base

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/fxamacker/cbor/v2"
	cid "github.com/ipfs/go-cid"
)

var Timestamp = time.Now

type treeInfo struct {
	name string // must be obtained from parent, match link name

	metadata Metadata
	previous *cid.Cid
	skeleton Skeleton
	userland cid.Cid
}

var (
	_ TreeHeader = (*treeInfo)(nil)
	_ Info       = (*treeInfo)(nil)
)

func (ti treeInfo) Stat() (fs.FileInfo, error) {
	return &FSFileInfo{
		name: ti.name,
		// size: ti.,
		mtime: time.Unix(int64(ti.metadata.UnixMeta.Mtime), 0),
	}, nil
}

func (ti treeInfo) Metadata() Metadata { return ti.metadata }
func (ti treeInfo) Previous() *cid.Cid { return ti.previous }
func (ti treeInfo) Skeleton() Skeleton { return ti.skeleton }
func (ti treeInfo) Userland() cid.Cid  { return ti.userland }

type fileInfo struct {
	metadata Metadata
	previous *cid.Cid
	userland cid.Cid
}

var _ Info = (*fileInfo)(nil)

func (fi fileInfo) Metadata() Metadata { return fi.metadata }
func (fi fileInfo) Previous() *cid.Cid { return fi.previous }
func (fi fileInfo) Userland() cid.Cid  { return fi.userland }

type FileInfo interface {
	fs.FileInfo
	Cid() cid.Cid
}

func Stat(f fs.File) (FileInfo, error) {
	afi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := afi.(FileInfo)
	if !ok {
		return nil, fmt.Errorf("expected base.FileInfo")
	}
	return st, nil
}

type FSFileInfo struct {
	name  string      // base name of the file
	size  int64       // length in bytes for regular files; system-dependent for others
	mode  fs.FileMode // file mode bits
	mtime time.Time   // modification time
	sys   interface{}

	cid cid.Cid // file identifier
}

func NewFSFileInfo(name string, size int64, mode fs.FileMode, mtime time.Time, sys interface{}) FSFileInfo {
	return FSFileInfo{
		name:  name,
		size:  size,
		mode:  mode,
		mtime: mtime,
		sys:   sys,
	}
}

func (fi FSFileInfo) Name() string       { return fi.name }
func (fi FSFileInfo) Size() int64        { return fi.size }
func (fi FSFileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi FSFileInfo) ModTime() time.Time { return fi.mtime }
func (fi FSFileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi FSFileInfo) Sys() interface{}   { return fi.sys }

func (fi *FSFileInfo) SetFilename(name string) error {
	fi.name = name
	return nil
}

type FSDirEntry struct {
	name   string
	isFile bool
}

var _ fs.DirEntry = (*FSDirEntry)(nil)

func NewFSDirEntry(name string, isFile bool) FSDirEntry {
	return FSDirEntry{
		name:   name,
		isFile: isFile,
	}
}

func (de FSDirEntry) Name() string { return de.name }
func (de FSDirEntry) IsDir() bool  { return !de.isFile }
func (ds FSDirEntry) Type() fs.FileMode {
	if ds.isFile {
		return 0
	}
	return fs.ModeDir
}
func (ds FSDirEntry) Info() (fs.FileInfo, error) { return nil, errors.New("FSDirEntry.FileInfo") }

// memfile is an in-memory file
type memfile struct {
	fi  os.FileInfo
	buf io.Reader
}

// Confirm that memfile satisfies the File interface
var _ = (fs.File)(&memfile{})

// NewFileWithInfo creates a new open file with provided file information
func NewFileWithInfo(fi fs.FileInfo, r io.Reader) (fs.File, error) {
	switch fi.Mode() {
	case os.ModeDir:
		return nil, fmt.Errorf("NewFileWithInfo doesn't support creating directories")
	default:
		return &memfile{
			fi:  fi,
			buf: r,
		}, nil
	}
}

// NewMemfileReader creates a file from an io.Reader
func NewMemfileReader(name string, r io.Reader) fs.File {
	return &memfile{
		fi: &FSFileInfo{
			name:  name,
			size:  int64(-1),
			mode:  0,
			mtime: Timestamp(),
		},
		buf: r,
	}
}

// NewMemfileBytes creates a file from a byte slice
func NewMemfileBytes(name string, data []byte) fs.File {
	return &memfile{
		fi: &FSFileInfo{
			name:  name,
			size:  int64(len(data)),
			mode:  0,
			mtime: Timestamp(),
		},
		buf: bytes.NewBuffer(data),
	}
}

// Stat returns information for this file
func (m memfile) Stat() (fs.FileInfo, error) {
	return m.fi, nil
}

// Read implements the io.Reader interface
func (m memfile) Read(p []byte) (int, error) {
	return m.buf.Read(p)
}

// Close closes the file, if the backing reader implements the io.Closer interface
// it will call close on the backing Reader
func (m memfile) Close() error {
	if closer, ok := m.buf.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type CBORFiler interface {
	CBORFile() (fs.File, error)
}

func DecodeCBOR(d []byte, v interface{}) error {
	return cbor.Unmarshal(d, v)
}

func EncodeCBOR(v interface{}) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	err := cbor.NewEncoder(buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf, err
}
