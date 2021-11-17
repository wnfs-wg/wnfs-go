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
	public "github.com/qri-io/wnfs-go/public"
	ratchet "github.com/qri-io/wnfs-go/ratchet"
)

var log = golog.Logger("wnfs")

const (
	// PreviousLinkName is the string for a historical backpointer in wnfs
	PreviousLinkName = "previous"
	// FileHierarchyNamePrivate is the root of encrypted files on WNFS
	FileHierarchyNamePrivate = "private"
	// FileHierarchyNamePublic is the root of public files on WNFS
	FileHierarchyNamePublic = "public"
	// FileHierarchyNamePretty is a link to a read-only branch at the root of a WNFS
	FileHierarchyNamePretty = "p"
)

type WNFS interface {
	fs.FS
	fs.ReadDirFile // wnfs root is a directory file
	PosixFS
	PrivateFS

	Cid() cid.Cid
	History(ctx context.Context, pathStr string, generations int) ([]HistoryEntry, error)
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
)

func NewEmptyFS(ctx context.Context, dagStore mdstore.MerkleDagStore, rs ratchet.Store, rootKey Key) (WNFS, error) {
	fs := &fileSystem{
		ctx:   ctx,
		store: dagStore,
	}

	root, err := newEmptyRootTree(fs, rs, rootKey)
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

func FromCID(ctx context.Context, dagStore mdstore.MerkleDagStore, rs ratchet.Store, id cid.Cid, rootKey Key, rootName PrivateName) (WNFS, error) {
	log.Debugw("FromCID", "cid", id)
	fs := &fileSystem{
		ctx:   ctx,
		store: dagStore,
	}

	root, err := newRootTreeFromCID(fs, rs, id, rootKey, rootName)
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
	return fsys.root.Stat()
}

func (fsys *fileSystem) Read(p []byte) (n int, err error) {
	return -1, errors.New("cannot read bytes of filsystem root")
}
func (fsys *fileSystem) Close() error { return nil }

func (fsys *fileSystem) ReadDir(n int) ([]fs.DirEntry, error) {
	return fsys.root.ReadDir(n)
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

func (fsys *fileSystem) History(ctx context.Context, pathStr string, max int) ([]HistoryEntry, error) {
	if pathStr == "." || pathStr == "" {
		return fsys.root.history(max)
	}
	node, relPath, err := fsys.fsHierarchyDirectoryNode(pathStr)
	if err != nil {
		return nil, err
	}

	f, err := node.Get(relPath)
	if err != nil {
		return nil, err
	}

	ch, ok := f.(base.Node)
	if !ok {
		return nil, fmt.Errorf("path %q is not a node", pathStr)
	}

	return ch.History(ctx, max)
}

func (fsys *fileSystem) fsHierarchyDirectoryNode(pathStr string) (dir base.Tree, relPath base.Path, err error) {
	path, err := base.NewPath(pathStr)
	if err != nil {
		return nil, path, err
	}

	head, tail := path.Shift()
	log.Debugw("fsHierarchyDirectoryNode", "head", head, "path", path)
	switch head {
	case "", ".":
		return fsys.root, tail, nil
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
	fs     base.MerkleDagFS
	pstore private.Store
	id     cid.Cid
	size   int64

	previous *cid.Cid
	Pretty   *base.BareTree
	Public   *public.PublicTree
	Private  *private.Root
}

var _ base.Tree = (*rootTree)(nil)

func newEmptyRootTree(fs base.MerkleDagFS, rs ratchet.Store, rootKey Key) (*rootTree, error) {
	root := &rootTree{
		fs:     fs,
		Public: public.NewEmptyTree(fs, FileHierarchyNamePublic),
		Pretty: &base.BareTree{},
	}

	privStore, err := private.NewStore(context.TODO(), fs.DagStore().Blockservice(), rs)
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

func newRootTreeFromCID(fs base.MerkleDagFS, rs ratchet.Store, id cid.Cid, rootKey Key, rootName PrivateName) (*rootTree, error) {
	ctx := context.TODO()
	node, err := fs.DagStore().GetNode(fs.Context(), id)
	if err != nil {
		return nil, fmt.Errorf("loading header block %q:\n%w", id.String(), err)
	}

	links := node.Links()

	var prev *cid.Cid
	if prevLink := links.Get(PreviousLinkName); prevLink != nil {
		prev = &prevLink.Cid
	}

	publicLink := links.Get(FileHierarchyNamePublic)
	if publicLink == nil {
		return nil, fmt.Errorf("root tree is missing %q link", FileHierarchyNamePublic)
	}

	public, err := public.LoadTree(ctx, fs, FileHierarchyNamePublic, publicLink.Cid)
	if err != nil {
		return nil, fmt.Errorf("opening /%s tree %s:\n%w", FileHierarchyNamePublic, publicLink.Cid, err)
	}

	// privStore, err := mdstore.NewPrivateStore(fs.Context(), fs.DagStore().Blockservice(), rs)

	var (
		privateRoot *private.Root
		privStore   private.Store
	)

	if hamtLink := links.Get(FileHierarchyNamePrivate); hamtLink != nil {
		privStore, err = private.LoadStore(context.TODO(), fs.DagStore().Blockservice(), rs, hamtLink.Cid)
		if err != nil {
			return nil, err
		}

		privateRoot, err = private.LoadRoot(fs.Context(), privStore, FileHierarchyNamePrivate, rootKey, rootName)
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
		fs:     fs,
		pstore: privStore,
		id:     id,

		Public:   public,
		Pretty:   &base.BareTree{}, // TODO(b5): finish pretty tree
		Private:  privateRoot,
		previous: prev,
	}

	return root, nil
}

func (r *rootTree) Put() (mdstore.PutResult, error) {
	if r.id.Equals(cid.Undef) {
		r.previous = &r.id
	}
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
func (t *rootTree) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   "",
		Cid:    t.id,
		Size:   t.size,
		IsFile: false,
		// TODO(b5):
		// Mtime:  t.metadata.UnixMeta.Mtime,
	}
}
func (r *rootTree) Links() mdstore.Links {
	links := mdstore.NewLinks(
		// mdstore.Link{Cid: r.Pretty, Size: r.Pretty.Size(), Name: FileHierarchyNamePretty},
		mdstore.Link{Cid: r.Public.Cid(), Size: r.Public.Size(), Name: FileHierarchyNamePublic},
		mdstore.Link{Cid: r.Private.Cid(), Size: r.Private.Size(), Name: FileHierarchyNamePrivate},
	)
	if r.previous != nil && !r.id.Equals(cid.Undef) {
		links.Add(mdstore.Link{Cid: *r.previous, Name: PreviousLinkName})
	}
	return links
}

func (r *rootTree) Get(path base.Path) (fs.File, error) {
	head, tail := path.Shift()
	log.Debugw("rootTree.Get", "head", head, "path", path)
	switch head {
	case "", ".":
		return r, nil
	case FileHierarchyNamePublic:
		return r.Public.Get(tail)
	case FileHierarchyNamePrivate:
		return r.Private.Get(tail)
	// case FileHierarchyNamePretty:
	// 	return fsys.root.Pretty, relPath, nil
	default:
		return nil, fmt.Errorf("%q is not a valid filesystem path", path)
	}
}

func (r *rootTree) Copy(path base.Path, srcPathStr string, srcFS fs.FS) (res base.PutResult, err error) {
	return nil, fmt.Errorf("cannot copy directly into root directory, only /public or /private")
}

func (r *rootTree) Add(path base.Path, f fs.File) (res base.PutResult, err error) {
	return nil, fmt.Errorf("cannot write directly into root directory, only /public or /private")
}

func (r *rootTree) Mkdir(path base.Path) (res base.PutResult, err error) {
	return nil, fmt.Errorf("cannot create directory within root. only /public or /private")
}

func (r *rootTree) Rm(path base.Path) (res base.PutResult, err error) {
	return nil, fmt.Errorf("cannot remove directory from root")
}

func (r *rootTree) Stat() (fi fs.FileInfo, err error) {
	return base.NewFSFileInfo(
		"",
		r.Size(),
		fs.ModeDir,
		// TODO (b5):
		// mtime: time.Unix(t.metadata.UnixMeta.Mtime, 0),
		time.Unix(0, 0),
		r.fs.DagStore(),
	), nil
}

func (r *rootTree) ReadDir(n int) ([]fs.DirEntry, error) {
	if n != -1 {
		return nil, errors.New("wnfs root only supports n= -1 for directory listing")
	}

	links := []fs.DirEntry{}
	if r.Private != nil {
		links = append(links, base.NewFSDirEntry(FileHierarchyNamePrivate, false))
	}
	if r.Public != nil {
		links = append(links, base.NewFSDirEntry(FileHierarchyNamePublic, false))
	}

	// TODO(b5): pretty dir

	return links, nil
}

func (r *rootTree) Read([]byte) (int, error) {
	return 0, fmt.Errorf("not a file")
}

func (r *rootTree) Close() error {
	return nil
}

func (r *rootTree) AsHistoryEntry() base.HistoryEntry {
	ent := base.HistoryEntry{
		Cid:  r.id,
		Size: r.size,
		// TODO(b5): add missing fields
		Previous: r.previous,
	}

	if r.Private != nil {
		if n, err := r.Private.PrivateName(); err == nil {
			ent.PrivateName = string(n)
		}
		ent.Key = r.Private.Key().Encode()
	}

	return ent
}

func (r *rootTree) History(context.Context, int) ([]base.HistoryEntry, error) {
	return nil, fmt.Errorf("unfinished: root tree History method")
}

func (r *rootTree) history(max int) (hist []base.HistoryEntry, err error) {
	ctx := r.fs.Context()
	store := r.fs.DagStore()

	hist = []base.HistoryEntry{
		r.AsHistoryEntry(),
	}

	var privHist []base.HistoryEntry
	if r.Private != nil {
		if privHist, err = r.Private.History(ctx, -1); err != nil {
			return hist, err
		}
		if len(privHist) >= 1 {
			privHist = privHist[1:]
		}
	}
	log.Debugw("private history", "history", privHist)

	prev := hist[0].Previous
	i := 0
	for prev != nil {
		nd, err := store.GetNode(ctx, *prev)
		if err != nil {
			return nil, err
		}
		ent := base.HistoryEntry{
			Cid: nd.Cid(),
			// TODO(b5): add missing fields
			Size: 0,
		}
		if lnk := nd.Links().Get(PreviousLinkName); lnk != nil {
			ent.Previous = &lnk.Cid
		}
		if len(privHist) > i {
			ent.Key = privHist[i].Key
			ent.PrivateName = privHist[i].PrivateName
		}

		i++
		hist = append(hist, ent)
		prev = ent.Previous

		if len(hist) == max {
			break
		}
	}

	return hist, nil
}

func (r *rootTree) MergeDiverged(n base.Node) (result base.MergeResult, err error) {
	return base.MergeResult{}, fmt.Errorf("unfinished: root tree MergeDiverged")
}

func Merge(ctx context.Context, aFs, bFs WNFS) (result base.MergeResult, err error) {
	a, ok := aFs.(*fileSystem)
	if !ok {
		return result, fmt.Errorf("'a' is not a wnfs filesystem")
	}
	b, ok := bFs.(*fileSystem)
	if !ok {
		return result, fmt.Errorf("'b' is not a wnfs filesystem")
	}
	log.Debugw("Merge", "acid", a.Cid(), "bcid", b.Cid())

	if a.root.Public != nil && b.root.Public != nil {
		res, err := public.Merge(ctx, a.root.Public, b.root.Public)
		if err != nil {
			return result, err
		}
		log.Debugw("merged public", "result", res.Cid)
		fmt.Printf("/public:\t%s\n", res.Type)
		a.root.Public, err = public.LoadTree(ctx, a.root.fs, FileHierarchyNamePublic, res.Cid)
		if err != nil {
			return base.MergeResult{}, err
		}
	}
	if a.root.Private != nil && b.root.Private != nil {
		res, err := private.Merge(ctx, a.root.Private, b.root.Private)
		if err != nil {
			return result, err
		}
		log.Debugw("merged private", "result", res.Cid)
		fmt.Printf("/private:\t%s\n", res.Type)
		pk := &private.Key{}
		if err := pk.Decode(res.Key); err != nil {
			return result, err
		}
		a.root.Private, err = private.LoadRoot(a.root.fs.Context(), a.root.pstore, FileHierarchyNamePrivate, *pk, private.Name(res.PrivateName))
		if err != nil {
			return result, err
		}
	}

	_, err = a.root.Put()

	// TODO(b5): populate result
	return result, nil
}
