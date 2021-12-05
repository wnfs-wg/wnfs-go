package wnfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"time"

	blocks "github.com/ipfs/go-block-format"
	blockservice "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	golog "github.com/ipfs/go-log"
	base "github.com/qri-io/wnfs-go/base"
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
	Node         = base.Node
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
	store public.Store
	ctx   context.Context
	root  *rootTree
}

var _ WNFS = (*fileSystem)(nil)

func NewEmptyFS(ctx context.Context, bserv blockservice.BlockService, rs ratchet.Store, rootKey Key) (WNFS, error) {
	store := public.NewStore(ctx, bserv)
	fs := &fileSystem{
		ctx:   ctx,
		store: store,
	}

	root, err := newEmptyRootTree(store, rs, rootKey)
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

func FromCID(ctx context.Context, bserv blockservice.BlockService, rs ratchet.Store, id cid.Cid, rootKey Key, rootName PrivateName) (WNFS, error) {
	log.Debugw("FromCID", "cid", id, "key", rootKey.Encode(), "name", string(rootName))
	store := public.NewStore(ctx, bserv)
	fs := &fileSystem{
		ctx:   ctx,
		store: store,
	}

	root, err := loadRoot(ctx, store, rs, id, rootKey, rootName)
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
func (fsys *fileSystem) Links() base.Links        { return fsys.root.Links() }

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
	tree, path, err := fsys.fsHierarchyDirectoryNode(pathStr)
	if err != nil {
		return nil, err
	}

	log.Debugw("fileSystem.Open", "pathStr", pathStr, "path", path)
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

	_, isDataFile := f.(StructuredDataFile)
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if fi.IsDir() && !isDataFile {
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

type rootHeader struct {
	Info     *public.Info
	Previous *cid.Cid
	Metadata *cid.Cid
	Pretty   *cid.Cid
	Public   *cid.Cid
	Private  *cid.Cid
}

func (h *rootHeader) encodeBlock() (blocks.Block, error) {
	header := map[string]interface{}{
		"info":               h.Info.Map(),
		"metadata":           h.Metadata,
		"previous":           h.Previous,
		base.PublicLinkName:  h.Public,
		base.PrivateLinkName: h.Private,
	}
	return cbornode.WrapObject(header, base.DefaultMultihashType, -1)
}

func decodeRootHeader(blk blocks.Block) (*rootHeader, error) {
	env := map[string]interface{}{}
	if err := cbornode.DecodeInto(blk.RawData(), &env); err != nil {
		return nil, err
	}

	info, ok := env["info"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("header block is missing info field")
	}
	h := &rootHeader{
		Info: public.InfoFromMap(info),
	}

	nd, err := cbornode.DecodeBlock(blk)
	if err != nil {
		return nil, err
	}

	for _, l := range nd.Links() {
		switch l.Name {
		case base.PreviousLinkName:
			h.Previous = &l.Cid
		case base.PublicLinkName:
			h.Public = &l.Cid
		case base.PrivateLinkName:
			h.Private = &l.Cid
		case base.PrettyLinkName:
			h.Pretty = &l.Cid
		case base.MetadataLinkName:
			h.Metadata = &l.Cid
		}
	}

	return h, nil
}

type rootTree struct {
	store   public.Store
	pstore  private.Store
	id      cid.Cid
	rootKey Key

	h *rootHeader

	// Pretty   *base.BareTree
	metadata *public.DataFile
	Public   *public.Tree
	Private  *private.Root
}

var _ base.Tree = (*rootTree)(nil)

func newEmptyRootTree(store public.Store, rs ratchet.Store, rootKey Key) (root *rootTree, err error) {
	root = &rootTree{
		store:   store,
		rootKey: rootKey,

		h: &rootHeader{
			Info: public.NewInfo(base.NTDir),
		},
		Public: public.NewEmptyTree(store, FileHierarchyNamePublic),
		// Pretty: &base.BareTree{},
	}

	root.pstore, err = private.NewStore(context.TODO(), store.Blockservice(), rs)
	if err != nil {
		return nil, err
	}

	privateRoot, err := private.NewEmptyRoot(store.Context(), root.pstore, FileHierarchyNamePrivate, rootKey)
	if err != nil {
		return nil, err
	}
	root.Private = privateRoot
	return root, nil
}

func loadRoot(ctx context.Context, store public.Store, rs ratchet.Store, id cid.Cid, rootKey Key, rootName PrivateName) (r *rootTree, err error) {
	r = &rootTree{store: store, id: id, rootKey: rootKey}

	blk, err := store.Blockservice().GetBlock(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("loading root header block: %w", err)
	}

	if r.h, err = decodeRootHeader(blk); err != nil {
		return nil, fmt.Errorf("decoding root header block: %w", err)
	}

	if r.h.Public != nil {
		if r.Public, err = public.LoadTree(ctx, store, FileHierarchyNamePublic, *r.h.Public); err != nil {
			return nil, fmt.Errorf("opening /%s tree %s:\n%w", FileHierarchyNamePublic, r.h.Public, err)
		}
	} else {
		r.Public = public.NewEmptyTree(store, FileHierarchyNamePublic)
	}

	if r.h.Private != nil && !rootKey.IsEmpty() {
		if r.pstore, err = private.LoadStore(ctx, store.Blockservice(), rs, *r.h.Private); err != nil {
			return nil, err
		}
		if r.Private, err = private.LoadRoot(store.Context(), r.pstore, FileHierarchyNamePrivate, rootKey, rootName); err != nil {
			return nil, fmt.Errorf("opening private root:\n%w", err)
		}
	} else {
		if r.pstore, err = private.LoadStore(ctx, store.Blockservice(), rs, cid.Undef); err != nil {
			return nil, err
		}
		if r.Private, err = private.LoadRoot(store.Context(), r.pstore, FileHierarchyNamePrivate, rootKey, rootName); err != nil {
			return nil, fmt.Errorf("opening private root:\n%w", err)
		}
	}

	return r, nil
}

func (r *rootTree) Put() (result base.PutResult, err error) {
	if r.id.Defined() {
		r.h.Previous = &r.id
	}
	r.h.Info.Mtime = base.Timestamp().Unix()
	if r.Public != nil {
		id := r.Public.Cid()
		r.h.Public = &id
	}
	if r.Private != nil {
		id := r.Private.Cid()
		r.h.Private = &id
	}

	if r.metadata != nil {
		if _, err = r.metadata.Put(); err != nil {
			return result, err
		}
		id := r.metadata.Cid()
		r.h.Metadata = &id
	}

	// TODO(by): build pretty link

	blk, err := r.h.encodeBlock()
	if err != nil {
		return result, fmt.Errorf("constructing root header block: %w", err)
	}
	if err = r.store.Blockservice().AddBlock(blk); err != nil {
		return result, fmt.Errorf("storing root header block: %w", err)
	}
	r.id = blk.Cid()
	log.Debugw("rootTree.Put", "linksLen", r.Links().Len(), "cid", r.id)
	return result, nil
}

func (r *rootTree) Cid() cid.Cid        { return r.id }
func (r *rootTree) Name() string        { return "wnfs" }
func (r *rootTree) Size() int64         { return r.h.Info.Size }
func (r *rootTree) IsDir() bool         { return true }
func (r *rootTree) Mode() fs.FileMode   { return fs.FileMode(r.h.Info.Mode) }
func (r *rootTree) Type() base.NodeType { return r.h.Info.Type }
func (r *rootTree) ModTime() time.Time  { return time.Unix(r.h.Info.Mtime, 0) }
func (r *rootTree) Sys() interface{}    { return r.store }

func (r *rootTree) SetMeta(md map[string]interface{}) error {
	r.metadata = public.NewDataFile(r.store, "", md)
	return nil
}

func (r *rootTree) Meta() (f base.LinkedDataFile, err error) {
	if r.metadata == nil && r.h.Metadata != nil {
		r.metadata, err = public.LoadDataFile(r.store.Context(), r.store, base.MetadataLinkName, *r.h.Metadata)
	}
	return r.metadata, err
}

func (r *rootTree) Links() base.Links {
	links := base.NewLinks(
		// base.Link{Cid: r.Pretty, Size: r.Pretty.Size(), Name: FileHierarchyNamePretty},
		base.Link{Cid: r.Public.Cid(), Size: r.Public.Size(), Name: FileHierarchyNamePublic},
		base.Link{Cid: r.Private.Cid(), Size: r.Private.Size(), Name: FileHierarchyNamePrivate},
	)

	if r.h.Previous != nil && !r.id.Equals(cid.Undef) {
		links.Add(base.Link{Cid: *r.h.Previous, Name: PreviousLinkName})
	}
	return links
}

func (r *rootTree) Get(path base.Path) (f fs.File, err error) {
	head, tail := path.Shift()
	log.Debugw("rootTree.Get", "head", head, "path", path)
	switch head {
	case "", ".":
		return r, nil
	case FileHierarchyNamePublic:
		if r.Public == nil {
			r.Public = public.NewEmptyTree(r.store, FileHierarchyNamePublic)
		}
		return r.Public.Get(tail)
	case FileHierarchyNamePrivate:
		if r.Private == nil {
			r.Private, err = private.NewEmptyRoot(r.store.Context(), r.pstore, FileHierarchyNamePrivate, r.rootKey)
			if err != nil {
				return nil, err
			}
		}
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
		r.store,
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
		Cid:      r.id,
		Size:     r.h.Info.Size,
		Mtime:    r.h.Info.Mtime,
		Type:     r.h.Info.Type,
		Previous: r.h.Previous,
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
	ctx := r.store.Context()
	store := r.store

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
		blk, err := store.Blockservice().GetBlock(ctx, *prev)
		if err != nil {
			return nil, err
		}
		h, err := decodeRootHeader(blk)
		if err != nil {
			return nil, err
		}
		ent := base.HistoryEntry{
			Cid:      *prev,
			Size:     h.Info.Size,
			Mtime:    h.Info.Mtime,
			Type:     h.Info.Type,
			Previous: h.Previous,
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
		a.root.Public, err = public.LoadTree(ctx, a.root.store, FileHierarchyNamePublic, res.Cid)
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
		a.root.Private, err = private.LoadRoot(a.root.store.Context(), a.root.pstore, FileHierarchyNamePrivate, *pk, private.Name(res.PrivateName))
		if err != nil {
			return result, err
		}
	}

	_, err = a.root.Put()

	// TODO(b5): populate result
	return result, nil
}

func HAMTContents(ctx context.Context, bs blockservice.BlockService, id cid.Cid) (map[string]string, error) {
	h, err := private.LoadHAMT(ctx, bs.Blockstore(), id)
	if err != nil {
		return nil, err
	}

	return h.Diagnostic(ctx), nil
}

type StructuredDataFile interface {
	fs.File
	Data() (interface{}, error)
}

type dataFile struct {
	name    string
	content interface{}
}

var (
	_ StructuredDataFile = (*dataFile)(nil)
	_ fs.FileInfo        = (*dataFile)(nil)
)

func NewDataFile(name string, data interface{}) StructuredDataFile {
	return &dataFile{
		name:    name,
		content: data,
	}
}

func (df *dataFile) Stat() (fs.FileInfo, error) { return df, nil }
func (df *dataFile) Name() string               { return df.name }
func (df *dataFile) IsDir() bool                { return true }
func (df *dataFile) Size() int64                { return -1 }
func (df *dataFile) ModTime() time.Time         { return time.Time{} }
func (df *dataFile) Mode() fs.FileMode          { return base.ModeDefault }
func (df *dataFile) Sys() interface{}           { return nil }

func (df *dataFile) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("not implemented: dataFile.Read")
}
func (df *dataFile) Close() error { return nil }
func (df *dataFile) Data() (interface{}, error) {
	// TODO(b5): horrible. This is just to coerce to usable types. In the real
	// world we need a cbor serialization lib that can handle arbitrary types
	// *and* recognizes cid.CID natively
	data, err := json.Marshal(df.content)
	if err != nil {
		return nil, err
	}
	res := map[string]interface{}{}
	err = json.Unmarshal(data, &res)
	return res, err
}

type Factory struct {
	BlockService blockservice.BlockService
	Ratchets     ratchet.Store
	Decryption   private.DecryptionStore
}

func (fac Factory) Load(ctx context.Context, id cid.Cid) (fs WNFS, err error) {
	var (
		name private.Name
		key  private.Key
	)

	if fac.Decryption != nil {
		name, key, err = fac.Decryption.DecryptionFields(id)
		if err != nil && !errors.Is(err, base.ErrNotFound) {
			return nil, err
		}
		err = nil
	}

	return FromCID(ctx, fac.BlockService, fac.Ratchets, id, key, name)
}

func (fac Factory) LoadWithDecryption(ctx context.Context, id cid.Cid, name private.Name, key private.Key) (fs WNFS, err error) {
	return FromCID(ctx, fac.BlockService, fac.Ratchets, id, key, name)
}

func NodeIsPrivate(n Node) bool {
	switch n.(type) {
	case *private.Root, *private.Tree, *private.File, *private.DataFile:
		return true
	default:
		return false
	}
}
