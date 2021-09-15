package wnfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"time"

	cid "github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log"
	base "github.com/qri-io/wnfs-go/base"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
	private "github.com/qri-io/wnfs-go/private"
	"github.com/qri-io/wnfs-go/public"
)

var log = golog.Logger("wnfs")

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
	PosixFS
	PrivateFS

	Cid() cid.Cid
	History(pathStr string, generations int) ([]HistoryEntry, error)
}

type PosixFS interface {
	// directories (trees)
	Ls(pathStr string) ([]fs.DirEntry, error)
	Mkdir(pathStr string, opts ...MutationOptions) error

	// files
	Write(pathStr string, f fs.File, opts ...MutationOptions) error
	Cat(pathStr string) ([]byte, error)
	Open(pathStr string) (fs.File, error)

	// general
	// Mv(from, to string) error
	Cp(pathStr, srcPathStr string, src fs.FS, opts ...MutationOptions) error
	Rm(pathStr string, opts ...MutationOptions) error
}

type (
	HistoryEntry = base.HistoryEntry
	// PrivateName abstracts the private package, providing a uniform interface
	// for wnfs that doesn't add a userland dependency
	PrivateName = private.Name
	// Key hoists up from the private package
	Key = private.Key
)

var NewKey = private.NewKey

type PrivateFS interface {
	RootKey() private.Key
	PrivateName() (PrivateName, error)
}

type MutationOptions struct {
	SourceFS fs.FS
	Commit   bool
}

func (o MutationOptions) assign(opts []MutationOptions) MutationOptions {
	for _, opt := range opts {
		o.Commit = opt.Commit
	}
	return o
}

type fileSystem struct {
	store mdstore.MerkleDagStore
	ctx   context.Context
	root  *rootTree
}

var (
	_ WNFS             = (*fileSystem)(nil)
	_ base.MerkleDagFS = (*fileSystem)(nil)
	_ mdstore.DagNode  = (*fileSystem)(nil)
)

func NewEmptyFS(ctx context.Context, dagStore mdstore.MerkleDagStore, rootKey Key) (WNFS, error) {
	fs := &fileSystem{
		ctx:   ctx,
		store: dagStore,
	}

	root, err := newEmptyRootTree(fs, rootKey)
	if err != nil {
		return nil, err
	}

	fs.root = root

	// put all root tree to establish base hashes for all top level directories in
	// the file hierarchy
	if _, err := root.Public.Put(); err != nil {
		return nil, err
	}
	if _, err := root.Private.Put(); err != nil {
		return nil, err
	}
	if _, err := root.Put(); err != nil {
		return nil, err
	}

	return fs, nil
}

func FromCID(ctx context.Context, dagStore mdstore.MerkleDagStore, id cid.Cid, rootKey Key, rootName PrivateName) (WNFS, error) {
	log.Debugw("FromCID", "cid", id)
	fs := &fileSystem{
		ctx:   ctx,
		store: dagStore,
	}

	root, err := newRootTreeFromCID(fs, id, rootKey, rootName)
	if err != nil {
		return nil, fmt.Errorf("opening root tree %s:\n%w", id, err)
	}

	fs.root = root
	return fs, nil
}

func (fsys *fileSystem) Context() context.Context { return fsys.ctx }
func (fsys *fileSystem) Name() string             { return fsys.root.Name() }
func (fsys *fileSystem) Cid() cid.Cid             { return fsys.root.Cid() }
func (fsys *fileSystem) Size() int64              { return fsys.root.Size() }
func (fsys *fileSystem) Links() mdstore.Links     { return fsys.root.Links() }

func (fsys *fileSystem) Stat() (fs.FileInfo, error) {
	return base.NewFSFileInfo(
		"",
		fsys.root.Size(),
		fs.ModeDir,
		// TODO (b5):
		// mtime: time.Unix(t.metadata.UnixMeta.Mtime, 0),
		time.Unix(0, 0),
		fsys.store,
	), nil
}

func (fsys *fileSystem) Read(p []byte) (n int, err error) {
	return -1, errors.New("cannot read bytes of filsystem root")
}
func (fsys *fileSystem) Close() error { return nil }

func (fsys *fileSystem) ReadDir(n int) ([]fs.DirEntry, error) {
	if n != -1 {
		return nil, errors.New("wnfs root only supports n= -1 for directory listing")
	}

	return []fs.DirEntry{
		base.NewFSDirEntry(FileHierarchyNamePublic, false),
	}, nil
}

func (fsys *fileSystem) RootKey() Key {
	return fsys.root.Private.Key()
}

func (fsys *fileSystem) PrivateName() (PrivateName, error) {
	pn, err := fsys.root.Private.PrivateName()
	if err != nil {
		return "", err
	}
	return pn, nil
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
		return nil, fmt.Errorf("opening %s:\n%w", pathStr, err)
	}
	return ioutil.ReadAll(f)
}

func (fsys *fileSystem) Cp(pathStr, srcPath string, src fs.FS, opts ...MutationOptions) error {
	log.Debugw("fileSystem.Cp", "pathStr", pathStr, "srcPath", srcPath)
	opt := MutationOptions{}.assign(opts)

	node, relPath, err := fsys.fsHierarchyDirectoryNode(pathStr)
	if err != nil {
		return err
	}

	if _, err := node.Copy(relPath, srcPath, src); err != nil {
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

func (fsys *fileSystem) Write(pathStr string, f fs.File, opts ...MutationOptions) error {
	log.Debugw("fileSystem.Write", "pathStr", pathStr)
	opt := MutationOptions{}.assign(opts)

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if fi.IsDir() {
		return errors.New("write only accepts files")
	}

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

func (fsys *fileSystem) History(pathStr string, max int) ([]HistoryEntry, error) {
	node, relPath, err := fsys.fsHierarchyDirectoryNode(pathStr)
	if err != nil {
		return nil, err
	}
	f, err := node.Get(relPath)
	if err != nil {
		return nil, err
	}

	fileNode, ok := f.(base.Node)
	if !ok {
		return nil, fmt.Errorf("node at %s doesn't support history", pathStr)
	}

	return base.History(fsys.ctx, fsys.store, fileNode, max)
}

func (fsys *fileSystem) fsHierarchyDirectoryNode(pathStr string) (dir base.Tree, relPath base.Path, err error) {
	path, err := base.NewPath(pathStr)
	if err != nil {
		return nil, path, err
	}

	head, tail := path.Shift()
	log.Debugw("fsHierarchyDirectoryNode", "head", head, "path", path)
	switch head {
	case FileHierarchyNamePublic:
		return fsys.root.Public, tail, nil
	case FileHierarchyNamePrivate:
		return fsys.root.Private, tail, nil
	// case FileHierarchyNamePretty:
	// 	return fsys.root.Pretty, relPath, nil
	default:
		return nil, path, fmt.Errorf("%q is not a valid filesystem path", path)
	}
}

type rootTree struct {
	fs   base.MerkleDagFS
	id   cid.Cid
	size int64

	Pretty  *base.BareTree
	Public  *public.PublicTree
	Private *private.Root
}

func newEmptyRootTree(fs base.MerkleDagFS, rootKey Key) (*rootTree, error) {
	root := &rootTree{
		fs:     fs,
		Public: public.NewEmptyTree(fs, FileHierarchyNamePublic),
		Pretty: &base.BareTree{},
	}

	privStore, err := mdstore.NewPrivateStore(fs.Context(), fs.DagStore().Blockservice())
	if err != nil {
		return nil, err
	}

	privateRoot, err := private.NewEmptyRoot(fs.Context(), privStore, FileHierarchyNamePrivate, rootKey)
	if err != nil {
		return nil, err
	}
	root.Private = privateRoot
	return root, nil
}

func newRootTreeFromCID(fs base.MerkleDagFS, id cid.Cid, rootKey Key, rootName PrivateName) (*rootTree, error) {
	node, err := fs.DagStore().GetNode(fs.Context(), id)
	if err != nil {
		return nil, fmt.Errorf("loading header block %q:\n%w", id.String(), err)
	}

	links := node.Links()

	publicLink := links.Get(FileHierarchyNamePublic)
	if publicLink == nil {
		return nil, fmt.Errorf("root tree is missing %q link", FileHierarchyNamePublic)
	}

	public, err := public.LoadTreeFromCID(fs, FileHierarchyNamePublic, publicLink.Cid)
	if err != nil {
		return nil, fmt.Errorf("opening /%s tree %s:\n%w", FileHierarchyNamePublic, publicLink.Cid, err)
	}

	privStore, err := mdstore.NewPrivateStore(fs.Context(), fs.DagStore().Blockservice())
	if err != nil {
		return nil, err
	}

	var privateRoot *private.Root

	if hamtLink := links.Get(FileHierarchyNamePrivate); hamtLink != nil {
		privateRoot, err = private.LoadRoot(fs.Context(), privStore, FileHierarchyNamePrivate, hamtLink.Cid, rootKey, rootName)
		if err != nil {
			return nil, fmt.Errorf("opening private tree:\n%w", err)
		}
	} else {
		privateRoot, err = private.NewEmptyRoot(fs.Context(), privStore, FileHierarchyNamePrivate, rootKey)
		if err != nil {
			return nil, err
		}
	}

	root := &rootTree{
		fs: fs,
		id: id,

		Public:  public,
		Pretty:  &base.BareTree{}, // TODO(b5): finish pretty tree
		Private: privateRoot,
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
	links := mdstore.NewLinks(
		// mdstore.LinkFromNode(r.Pretty, FileHierarchyNamePretty, false),
		mdstore.LinkFromNode(r.Public, FileHierarchyNamePublic, false),
		mdstore.LinkFromNode(r.Private, FileHierarchyNamePrivate, false),
	)
	return links
}
