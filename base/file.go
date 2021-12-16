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
	ipldcbor "github.com/ipfs/go-ipld-cbor"
)

var Timestamp = time.Now

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

func toSaneMap(n map[interface{}]interface{}) (interface{}, error) {
	out := make(map[string]interface{})
	for k, v := range n {
		ks, ok := k.(string)
		if !ok {
			return nil, ipldcbor.ErrInvalidKeys
		}

		obj, err := SanitizeCBORForJSON(v)
		if err != nil {
			return nil, err
		}

		out[ks] = obj
	}

	return out, nil
}

func SanitizeCBORForJSON(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case map[interface{}]interface{}:
		return toSaneMap(v)
	case []interface{}:
		var out []interface{}
		if len(v) == 0 && v != nil {
			return []interface{}{}, nil
		}
		for _, i := range v {
			obj, err := SanitizeCBORForJSON(i)
			if err != nil {
				return nil, err
			}

			out = append(out, obj)
		}
		return out, nil
	default:
		return v, nil
	}
}
