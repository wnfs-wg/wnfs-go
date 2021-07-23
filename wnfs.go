package wnfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
	cid "github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log"
	"github.com/qri-io/wnfs-go/mdstore"
)

var (
	log         = golog.Logger("wnfs")
	Timestamp   = time.Now
	ErrNotFound = errors.New("not found")
)

// LatestVersion is the most recent semantic version of WNFS this implementation
// can read/write
const LatestVersion = SemVer("1.0.0")

const (
	// FileHierarchyNamePublic is the root of public files on WNFS
	FileHierarchyNamePublic = "public"
	// FileHierarchyNamePrivate is the root of encrypted files on WNFS
	FileHierarchyNamePrivate = "private"
	// FileHierarchyNamePretty is a link to a read-only branch at the root of a WNFS
	FileHierarchyNamePretty = "p"
)

type WNFS interface {
	fs.FS
	fs.ReadDirFile // wnfs root is a directory file
	PosixTree
}

type PosixTree interface {
	// directories (trees)
	Ls(pathStr string) ([]fs.DirEntry, error)
	Mkdir(pathStr string, opts ...MutationOptions) error

	// files
	Write(pathStr string, f fs.File, opts ...MutationOptions) error
	Cat(pathStr string) ([]byte, error)
	Open(pathStr string) (fs.File, error)

	// general
	// Mv(from, to string) error
	Rm(pathStr string, opts ...MutationOptions) error
}

type MutationOptions struct {
	Commit bool
}

func (o MutationOptions) assign(opts []MutationOptions) MutationOptions {
	for _, opt := range opts {
		o.Commit = opt.Commit
	}
	return o
}

type merkleDagFS interface {
	fs.FS
	DagStore() mdstore.MerkleDagStore
}

type fileSystem struct {
	store mdstore.MerkleDagStore
	ctx   context.Context
	root  *rootTree
}

var (
	_ WNFS            = (*fileSystem)(nil)
	_ merkleDagFS     = (*fileSystem)(nil)
	_ mdstore.DagNode = (*fileSystem)(nil)
)

func NewEmptyFS(ctx context.Context, dagStore mdstore.MerkleDagStore) (WNFS, error) {
	fs := &fileSystem{
		ctx:   ctx,
		store: dagStore,
	}

	root, err := newEmptyRootTree(fs, "")
	if err != nil {
		return nil, err
	}

	fs.root = root
	return fs, nil
}

func FromCID(ctx context.Context, dagStore mdstore.MerkleDagStore, id cid.Cid) (WNFS, error) {
	fs := &fileSystem{
		ctx:   ctx,
		store: dagStore,
	}

	root, err := newRootTreeFromCID(fs, id)
	if err != nil {
		return nil, err
	}

	fs.root = root
	return fs, nil
}

func (fsys *fileSystem) Name() string         { return fsys.root.Name() }
func (fsys *fileSystem) Cid() cid.Cid         { return fsys.root.Cid() }
func (fsys *fileSystem) Size() int64          { return fsys.root.Size() }
func (fsys *fileSystem) Links() mdstore.Links { return fsys.root.Links() }

func (fsys *fileSystem) Stat() (fs.FileInfo, error) {
	return &fsFileInfo{
		size: fsys.root.Size(),
		// mode:  fsys.root.metadata.UnixMeta.Mode,
		// mtime: time.Unix(t.metadata.UnixMeta.Mtime, 0),
		sys: fsys.store,
	}, nil
}

func (fsys *fileSystem) IsDir() bool { return true }

func (fsys *fileSystem) Read(p []byte) (n int, err error) {
	return -1, errors.New("cannot read bytes of filsystem root")
}
func (fsys *fileSystem) Close() error { return nil }

func (fsys *fileSystem) ReadDir(n int) ([]fs.DirEntry, error) {
	if n != -1 {
		return nil, errors.New("wnfs root only supports n= -1 for directory listing")
	}

	return []fs.DirEntry{
		fsDirEntry{name: FileHierarchyNamePublic},
	}, nil
}

func (fsys *fileSystem) DagStore() mdstore.MerkleDagStore {
	return fsys.store
}

func (fsys *fileSystem) Ls(pathStr string) ([]fs.DirEntry, error) {
	log.Debugw("fileSystem.Ls", "pathStr", pathStr)
	tree, path, err := fsys.fsHierarchyDirectoryNode(pathStr)
	if err != nil {
		return nil, err
	}

	var file fs.File
	if head, _ := path.Shift(); head == "" {
		file = tree.(fs.File)
	} else {
		file, err = tree.Get(path)
		if err != nil {
			return nil, err
		}
	}

	dir, ok := file.(fs.ReadDirFile)
	if !ok {
		return nil, fmt.Errorf("path %q is not a directory", pathStr)
	}

	return dir.ReadDir(-1)
}

func (fsys *fileSystem) Mkdir(pathStr string, opts ...MutationOptions) error {
	log.Debugw("fileSystem.Mkdir", "pathStr", pathStr)
	opt := MutationOptions{}.assign(opts)

	tree, path, err := fsys.fsHierarchyDirectoryNode(pathStr)
	if err != nil {
		return err
	}

	tree.Mkdir(path)

	if opt.Commit {
		_, err := fsys.root.Put()
		if err != nil {
			return err
		}
	}

	return nil
}

func (fsys *fileSystem) Open(pathStr string) (fs.File, error) {
	log.Debugw("fileSystem.Open", "pathStr", pathStr)
	tree, path, err := fsys.fsHierarchyDirectoryNode(pathStr)
	if err != nil {
		return nil, err
	}

	return tree.Get(path)
}

func (fsys *fileSystem) Cat(pathStr string) ([]byte, error) {
	f, err := fsys.Open(pathStr)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(f)
}

func (fsys *fileSystem) Write(pathStr string, f fs.File, opts ...MutationOptions) error {
	log.Debugw("fileSystem.Write", "pathStr", pathStr)
	opt := MutationOptions{}.assign(opts)

	// fi, err := f.Stat()
	// if err != nil {
	// 	return err
	// }
	// if fi.IsDir() {
	// 	return errors.New("write only accepts file paths")
	// }

	node, relPath, err := fsys.fsHierarchyDirectoryNode(pathStr)
	if err != nil {
		return err
	}

	if _, err := node.Add(relPath, f); err != nil {
		return err
	}

	if opt.Commit {
		_, err := fsys.root.Put()
		if err != nil {
			return err
		}
	}

	return nil
}

func (fsys *fileSystem) Rm(pathStr string, opts ...MutationOptions) error {
	log.Debugw("fileSystem.Rm", "pathStr", pathStr)
	opt := MutationOptions{}.assign(opts)

	tree, relPath, err := fsys.fsHierarchyDirectoryNode(pathStr)
	if err != nil {
		return err
	}

	if _, err := tree.Rm(relPath); err != nil {
		return err
	}

	if opt.Commit {
		_, err := fsys.root.Put()
		if err != nil {
			return err
		}
	}

	return nil
}

func (fsys *fileSystem) fsHierarchyDirectoryNode(pathStr string) (dir Tree, relPath Path, err error) {
	path, err := NewPath(pathStr)
	if err != nil {
		return nil, path, err
	}

	head, tail := path.Shift()
	switch head {
	case FileHierarchyNamePublic:
		return fsys.root.Public, tail, nil
	case FileHierarchyNamePrivate:
		return nil, tail, errors.New("private files are not implemented in wnfs-go")
	// 	return fsys.root.Private, relPath, nil
	// case FileHierarchyNamePretty:
	// 	return fsys.root.Pretty, relPath, nil
	default:
		return nil, path, errors.New("not a valid filesystem path")
	}
}

func filename(file fs.File) (string, error) {
	fi, err := file.Stat()
	if err != nil {
		return "", err
	}
	return fi.Name(), nil
}

type Path []string

func NewPath(posix string) (Path, error) {
	return strings.Split(posix, "/"), nil
}

func (p Path) Shift() (head string, ch Path) {
	switch len(p) {
	case 0:
		return "", nil
	case 1:
		return p[0], nil
	default:
		return p[0], p[1:]
	}
}

type File interface {
	IsFile() bool
	UpdateContent(f fs.File)
}

type Tree interface {
	IsFile() bool
	Get(path Path) (fs.File, error)
	Add(path Path, f fs.File) (putResult, error)
	Rm(path Path) (putResult, error)
	Mkdir(path Path) (putResult, error)
}

type Node interface {
	IsFile() bool
}

type rootTree struct {
	fs   merkleDagFS
	id   cid.Cid
	size int64

	Pretty  *BareTree
	Public  *PublicTree
	Private *PrivateTree
	MMPT    *MMPT
}

func newEmptyRootTree(fs merkleDagFS, rootKey string) (*rootTree, error) {
	root := &rootTree{
		fs: fs,

		Public: newEmptyPublicTree(fs, FileHierarchyNamePublic),
		Pretty: &BareTree{},
		// Private: *PrivateTree,
		MMPT: &MMPT{},
	}

	return root, nil
}

func newRootTreeFromCID(fs merkleDagFS, id cid.Cid) (*rootTree, error) {
	node, err := fs.DagStore().GetNode(id)
	if err != nil {
		return nil, fmt.Errorf("loading header block %q: %w", id.String(), err)
	}

	links := node.Links()

	publicLink := links.Get(FileHierarchyNamePublic)
	if publicLink == nil {
		return nil, fmt.Errorf("root tree is missing %q link", FileHierarchyNamePublic)
	}

	public, err := loadTreeFromCID(fs, FileHierarchyNamePublic, publicLink.Cid)
	if err != nil {
		return nil, err
	}

	root := &rootTree{
		fs: fs,
		id: id,

		Public: public,
		Pretty: &BareTree{}, // TODO
		MMPT:   &MMPT{},
	}

	return root, nil
}

func (r *rootTree) Put() (mdstore.PutResult, error) {
	result, err := r.fs.DagStore().PutNode(r.Links())
	if err != nil {
		return result, err
	}
	r.id = result.Cid
	log.Debugw("rootTree.put", "linksLen", r.Links().Len(), "cid", r.id)
	return result, nil
}

func (r *rootTree) Cid() cid.Cid { return r.id }
func (r *rootTree) Name() string { return "wnfs" }
func (r *rootTree) Size() int64  { return r.size }
func (r *rootTree) Links() mdstore.Links {
	return mdstore.NewLinks(
		// mdstore.LinkFromNode(r.Pretty, FileHierarchyNamePretty, false),
		mdstore.LinkFromNode(r.Public, FileHierarchyNamePublic, false),
		// r.Private,
		// r.MMPT,
	)
}

type CBORFiler interface {
	CBORFile(encKey *string) (fs.File, error)
}

func decodeCBOR(d []byte, v interface{}) error {
	return cbor.Unmarshal(d, v)
}

func encodeCBOR(v interface{}, encKey *string) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	err := cbor.NewEncoder(buf).Encode(v)
	if err != nil {
		return nil, err
	}

	if encKey != nil {
		buf = bytes.NewBuffer(aesEncrypt(buf.Bytes(), *encKey))
	}
	return buf, err
}

func aesEncrypt(d []byte, key string) []byte {
	panic("unfinished: aesEncrypt")
	// return d
}
