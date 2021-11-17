package public

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	golog "github.com/ipfs/go-log"
	"github.com/multiformats/go-multihash"
	"github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/mdstore"
)

var log = golog.Logger("wnfs")

type header struct {
	Info     *Info
	Previous *cid.Cid // historical backpointer
	Merge    *cid.Cid // if this version is a merge, will be populated
	Metadata *cid.Cid
	Skeleton *cid.Cid // only present on directories
	Userland *cid.Cid
}

func loadHeader(ctx context.Context, s mdstore.MerkleDagStore, id cid.Cid) (*header, error) {
	blk, err := s.Blockservice().GetBlock(ctx, id)
	if err != nil {
		return nil, err
	}

	return decodeHeaderBlock(blk)
}

func decodeHeaderBlock(blk blocks.Block) (*header, error) {
	env := map[string]interface{}{}
	if err := cbornode.DecodeInto(blk.RawData(), &env); err != nil {
		return nil, err
	}

	nd, err := cbornode.DecodeBlock(blk)
	if err != nil {
		return nil, err
	}

	log.Debugw("decodeDataFileBlock", "info", env["info"], "env", env)

	info, ok := env["info"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("header block is missing info field")
	}
	h := &header{
		Info: infoFromMap(info),
	}

	for _, l := range nd.Links() {
		log.Debugw("setting link field", "name", l.Name, "cid", l.Cid)
		switch l.Name {
		case base.PreviousLinkName:
			h.Previous = &l.Cid
		case base.MergeLinkName:
			h.Merge = &l.Cid
		case base.MetadataLinkName:
			h.Metadata = &l.Cid
		case base.SkeletonLinkName:
			h.Skeleton = &l.Cid
		case base.UserlandLinkName:
			h.Userland = &l.Cid
		}
	}

	return h, nil
}

func (h *header) encodeBlock() (blocks.Block, error) {
	dataFile := map[string]interface{}{
		"metadata": h.Metadata,
		"previous": h.Previous,
		"merge":    h.Merge,
		"skeleton": h.Skeleton,
		"userland": h.Userland,
	}

	if h.Info != nil {
		dataFile["info"] = h.Info.Map()
	}
	return cbornode.WrapObject(dataFile, multihash.SHA2_256, -1)
}

func (h *header) loadMetadata(fs base.MerkleDagFS) (*base.Metadata, error) {
	if h.Metadata == nil {
		return nil, fmt.Errorf("header is missing %s link", base.MetadataLinkName)
	}

	md, err := base.LoadMetadata(fs.Context(), fs.DagStore(), *h.Metadata)
	if err != nil {
		return nil, fmt.Errorf("loading %s data %s:\n%w", base.MetadataLinkName, h.Metadata, err)
	}

	return md, nil
}

func (h *header) links() mdstore.Links {
	links := mdstore.NewLinks()

	if h.Previous != nil {
		links.Add(mdstore.Link{Name: base.PreviousLinkName, Cid: *h.Previous})
	}
	if h.Merge != nil {
		links.Add(mdstore.Link{Name: base.MergeLinkName, Cid: *h.Merge})
	}
	if h.Metadata != nil {
		links.Add(mdstore.Link{Name: base.MetadataLinkName, Cid: *h.Metadata})
	}
	if h.Userland != nil {
		links.Add(mdstore.Link{Name: base.UserlandLinkName, Cid: *h.Userland})
	}
	if h.Skeleton != nil {
		links.Add(mdstore.Link{Name: base.SkeletonLinkName, Cid: *h.Skeleton})
	}

	return links
}

type Info struct {
	WNFS  base.SemVer   `json:"wnfs"`
	Type  base.NodeType `json:"type"`
	Mode  uint32        `json:"mode"`
	Ctime int64         `json:"ctime"`
	Mtime int64         `json:"mtime"`
	Size  int64         `json:"size"`
}

func (i *Info) Map() map[string]interface{} {
	return map[string]interface{}{
		"wnfs":  i.WNFS,
		"type":  i.Type,
		"mode":  i.Mode,
		"ctime": i.Ctime,
		"mtime": i.Mtime,
		"size":  i.Size,
	}
}

func infoFromMap(m map[string]interface{}) *Info {
	i := &Info{}
	if version, ok := m["wnfs"].(string); ok {
		i.WNFS = base.SemVer(version)
	}
	if t, ok := m["type"].(int); ok {
		i.Type = base.NodeType(t)
	}
	if mode, ok := m["mode"].(int); ok {
		i.Mode = uint32(mode)
	}
	if ctime, ok := m["ctime"].(int); ok {
		i.Ctime = int64(ctime)
	}
	if mtime, ok := m["mtime"].(int); ok {
		i.Mtime = int64(mtime)
	}
	if size, ok := m["size"].(int); ok {
		i.Size = int64(size)
	}
	return i
}

type PublicTree struct {
	fs   base.MerkleDagFS // embed a reference to store this tree is associated with
	name string           // directory name, used while linking
	cid  cid.Cid
	h    *header

	metadata *base.Metadata
	skeleton base.Skeleton
	userland mdstore.Links // links to files are stored in "userland" header key
}

var (
	_ mdstore.DagNode     = (*PublicTree)(nil)
	_ base.Tree           = (*PublicTree)(nil)
	_ base.SkeletonSource = (*PublicTree)(nil)
	_ fs.File             = (*PublicTree)(nil)
	_ base.FileInfo       = (*PublicTree)(nil)
	_ fs.ReadDirFile      = (*PublicTree)(nil)
)

func NewEmptyTree(fs base.MerkleDagFS, name string) *PublicTree {
	now := base.Timestamp().Unix()
	return &PublicTree{
		fs:   fs,
		name: name,
		h: &header{
			Info: &Info{
				WNFS:  base.LatestVersion,
				Type:  base.NTDir,
				Mode:  base.ModeDefault,
				Ctime: now,
				Mtime: now,
				Size:  -1,
			},
		},

		userland: mdstore.NewLinks(),
		skeleton: base.Skeleton{},
	}
}

func LoadTree(ctx context.Context, fs base.MerkleDagFS, name string, id cid.Cid) (*PublicTree, error) {
	log.Debugw("loadTree", "name", name, "cid", id)

	h, err := loadHeader(ctx, fs.DagStore(), id)
	if err != nil {
		return nil, err
	}

	return treeFromHeader(ctx, fs, h, name, id)
}

func treeFromHeader(ctx context.Context, fs base.MerkleDagFS, h *header, name string, id cid.Cid) (*PublicTree, error) {
	store := fs.DagStore()
	if h.Info.Type != base.NTDir {
		return nil, fmt.Errorf("expected file to be a tree")
	}
	if h.Skeleton == nil {
		return nil, fmt.Errorf("header is missing %s link", base.SkeletonLinkName)
	}
	sk, err := base.LoadSkeleton(ctx, store, *h.Skeleton)
	if err != nil {
		return nil, fmt.Errorf("loading %s data %s:\n%w", base.SkeletonLinkName, h.Skeleton, err)
	}

	if h.Userland == nil {
		return nil, fmt.Errorf("header is missing %s link", base.UserlandLinkName)
	}
	userland, err := store.GetNode(ctx, *h.Userland)
	if err != nil {
		return nil, fmt.Errorf("loading %s data %s:\n%w", base.UserlandLinkName, h.Userland, err)
	}

	return &PublicTree{
		fs:   fs,
		name: name,
		cid:  id,

		h: h,
		// metadata: md,
		skeleton: sk,
		userland: userland.Links(),
	}, nil
}

func (t *PublicTree) Links() mdstore.Links       { return t.userland }
func (t *PublicTree) Raw() []byte                { return nil }
func (t *PublicTree) Name() string               { return t.name }
func (t *PublicTree) Size() int64                { return t.h.Info.Size }
func (t *PublicTree) ModTime() time.Time         { return time.Unix(t.h.Info.Ctime, 0) }
func (t *PublicTree) Mode() fs.FileMode          { return fs.FileMode(t.h.Info.Mode) }
func (t *PublicTree) IsDir() bool                { return true }
func (t *PublicTree) Sys() interface{}           { return t.fs }
func (t *PublicTree) Store() base.MerkleDagFS    { return t.fs }
func (t *PublicTree) Cid() cid.Cid               { return t.cid }
func (t *PublicTree) Stat() (fs.FileInfo, error) { return t, nil }

func (t *PublicTree) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   t.name,
		Cid:    t.cid,
		Size:   t.h.Info.Size,
		IsFile: false,
		Mtime:  t.h.Info.Mtime,
	}
}

func (t *PublicTree) Read(p []byte) (n int, err error) {
	return -1, errors.New("cannot read directory")
}
func (t *PublicTree) Close() error { return nil }

func (t *PublicTree) ReadDir(n int) ([]fs.DirEntry, error) {
	if n < 0 {
		n = t.userland.Len()
	}

	entries := make([]fs.DirEntry, 0, n)
	for i, link := range t.userland.SortedSlice() {
		entries = append(entries, base.NewFSDirEntry(link.Name, link.IsFile))

		if i == n {
			break
		}
	}
	return entries, nil
}

func (t *PublicTree) Skeleton() (base.Skeleton, error) {
	return t.skeleton, nil
}

func (t *PublicTree) Get(path base.Path) (fs.File, error) {
	ctx := context.TODO()
	head, tail := path.Shift()
	if head == "" {
		return t, nil
	}

	link := t.userland.Get(head)
	if link == nil {
		return nil, base.ErrNotFound
	}

	if tail != nil {
		ch, err := LoadTree(ctx, t.fs, head, link.Cid)
		if err != nil {
			return nil, err
		}

		// recurse
		return ch.Get(tail)
	}

	if t.skeleton[head].IsFile {
		return LoadFile(ctx, t.fs, link.Name, link.Cid)
	}

	return LoadTree(ctx, t.fs, link.Name, link.Cid)
}

func (t *PublicTree) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		Cid:      t.cid,
		Previous: t.h.Previous,
		Size:     t.h.Info.Size,
		Type:     t.h.Info.Type,
		Mtime:    t.h.Info.Mtime,
	}
}

func (t *PublicTree) Mkdir(path base.Path) (res base.PutResult, err error) {
	if len(path) < 1 {
		return res, errors.New("invalid path: empty")
	}

	head, tail := path.Shift()
	childDir, err := t.getOrCreateDirectChildTree(head)
	if err != nil {
		return nil, err
	}

	if tail == nil {
		res, err = childDir.Put()
		if err != nil {
			return nil, err
		}
	} else {
		res, err = t.Mkdir(tail)
		if err != nil {
			return nil, err
		}
	}

	t.updateUserlandLink(head, res)
	return t.Put()
}

func (t *PublicTree) Add(path base.Path, f fs.File) (res base.PutResult, err error) {
	if len(path) == 0 {
		return res, errors.New("invalid path: empty")
	}

	head, tail := path.Shift()
	if tail == nil {
		res, err = t.createOrUpdateChildFile(head, f)
		if err != nil {
			return res, err
		}
	} else {
		childDir, err := t.getOrCreateDirectChildTree(head)
		if err != nil {
			return res, err
		}

		// recurse
		res, err = childDir.Add(tail, f)
		if err != nil {
			return res, err
		}
	}

	t.updateUserlandLink(head, res)
	// contents of tree have changed, write an update.
	// TODO(b5) - pretty sure this is a bug if multiple writes are batched in the
	// same "publish" / transaction. Write advances the previous / current CID,
	// so if the same directory is mutated multiple times before the next snapshot
	// we'll have intermediate states as the "previous" pointer
	return t.Put()
}

func (t *PublicTree) Copy(path base.Path, srcPathStr string, srcFS fs.FS) (res base.PutResult, err error) {
	log.Debugw("PublicTree.copy", "path", path, "srcPath", srcPathStr)
	if len(path) == 0 {
		return res, errors.New("invalid path: empty")
	}

	head, tail := path.Shift()
	if tail == nil {
		f, err := srcFS.Open(srcPathStr)
		if err != nil {
			return nil, err
		}

		res, err = t.createOrUpdateChild(srcPathStr, head, f, srcFS)
		if err != nil {
			return res, err
		}
	} else {
		childDir, err := t.getOrCreateDirectChildTree(head)
		if err != nil {
			return res, err
		}

		// recurse
		res, err = childDir.Copy(tail, srcPathStr, srcFS)
		if err != nil {
			return res, err
		}
	}

	t.updateUserlandLink(head, res)
	// contents of tree have changed, write an update.
	// TODO(b5) - pretty sure this is a bug if multiple writes are batched in the
	// same "publish" / transaction. Write advances the previous / current CID,
	// so if the same directory is mutated multiple times before the next snapshot
	// we'll have intermediate states as the "previous" pointer
	return t.Put()
}

func (t *PublicTree) Rm(path base.Path) (base.PutResult, error) {
	ctx := context.TODO()
	head, tail := path.Shift()
	if head == "" {
		return PutResult{}, fmt.Errorf("invalid path: empty")
	}

	if tail == nil {
		t.removeUserlandLink(head)
	} else {
		link := t.userland.Get(head)
		if link == nil {
			return PutResult{}, base.ErrNotFound
		}
		child, err := LoadTree(ctx, t.fs, head, link.Cid)
		if err != nil {
			return nil, err
		}

		// recurse
		res, err := child.Rm(tail)
		if err != nil {
			return nil, err
		}
		t.updateUserlandLink(head, res)
	}

	// contents of tree have changed, write an update.
	return t.Put()
}

func (t *PublicTree) Put() (base.PutResult, error) {
	store := t.fs.DagStore()

	userlandResult, err := store.PutNode(t.userland)
	if err != nil {
		return nil, err
	}
	t.h.Userland = &userlandResult.Cid

	if t.metadata != nil {
		metaBuf, err := base.EncodeCBOR(t.metadata)
		if err != nil {
			return nil, err
		}
		id, err := store.PutBlock(metaBuf.Bytes())
		if err != nil {
			return nil, err
		}
		t.h.Metadata = &id
	}

	skf, err := t.skeleton.CBORFile()
	if err != nil {
		return nil, err
	}
	res, err := store.PutFile(skf)
	if err != nil {
		return nil, err
	}
	t.h.Skeleton = &res.Cid

	if t.cid.Defined() {
		// need to copy CID, as we're about to alter it's value
		id, _ := cid.Parse(t.cid)
		t.h.Previous = &id
	}

	blk, err := t.h.encodeBlock()
	if err != nil {
		return PutResult{}, err
	}

	if err := t.fs.DagStore().Blockservice().Blockstore().Put(blk); err != nil {
		return PutResult{}, err
	}

	t.cid = blk.Cid()
	log.Debugw("wrote public tree", "name", t.name, "cid", t.cid.String(), "userlandLinkCount", t.userland.Len(), "size", t.h.Info.Size, "prev", t.h.Previous)

	return PutResult{
		Cid:  t.cid,
		Size: t.h.Info.Size,
		// Metadata: *t.h.Metadata,
		Userland: *t.h.Userland,
		Skeleton: t.skeleton,
	}, nil
}

func (t *PublicTree) History(ctx context.Context, max int) ([]base.HistoryEntry, error) {
	return history(ctx, t, max)
}

func history(ctx context.Context, n base.Node, max int) ([]base.HistoryEntry, error) {
	store, err := base.NodeFS(n)
	if err != nil {
		return nil, err
	}

	log := []base.HistoryEntry{
		n.AsHistoryEntry(),
	}

	prev := log[0].Previous
	for prev != nil {
		ent, err := loadHistoryEntry(ctx, store.DagStore(), *prev)
		if err != nil {
			return nil, err
		}
		log = append(log, ent)
		prev = ent.Previous

		if len(log) == max {
			break
		}
	}

	return log, nil
}

func loadHistoryEntry(ctx context.Context, store mdstore.MerkleDagStore, id cid.Cid) (base.HistoryEntry, error) {
	h, err := loadHeader(ctx, store, id)
	if err != nil {
		return base.HistoryEntry{}, err
	}

	return base.HistoryEntry{
		Cid:      id,
		Previous: h.Previous,
		Type:     h.Info.Type,
		Mtime:    h.Info.Mtime,
		Size:     h.Info.Size,
	}, nil
}

func (t *PublicTree) getOrCreateDirectChildTree(name string) (*PublicTree, error) {
	ctx := context.TODO()
	link := t.userland.Get(name)
	if link == nil {
		return NewEmptyTree(t.fs, name), nil
	}
	return LoadTree(ctx, t.fs, name, link.Cid)
}

func (t *PublicTree) createOrUpdateChild(srcPathStr, name string, f fs.File, srcFS fs.FS) (base.PutResult, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return t.createOrUpdateChildDirectory(srcPathStr, name, f, srcFS)
	}
	return t.createOrUpdateChildFile(name, f)
}

func (t *PublicTree) createOrUpdateChildDirectory(srcPathStr, name string, f fs.File, srcFS fs.FS) (base.PutResult, error) {
	ctx := context.TODO()
	dir, ok := f.(fs.ReadDirFile)
	if !ok {
		return nil, fmt.Errorf("cannot read directory contents")
	}
	ents, err := dir.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("reading directory contents: %w", err)
	}

	var tree *PublicTree
	if link := t.userland.Get(name); link != nil {
		tree, err = LoadTree(ctx, t.fs, name, link.Cid)
		if err != nil {
			return nil, err
		}
	} else {
		tree = NewEmptyTree(t.fs, name)
	}

	var res base.PutResult
	for _, ent := range ents {
		res, err = tree.Copy(base.Path{ent.Name()}, filepath.Join(srcPathStr, ent.Name()), srcFS)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (t *PublicTree) createOrUpdateChildFile(name string, f fs.File) (base.PutResult, error) {
	ctx := context.TODO()
	if link := t.userland.Get(name); link != nil {
		previousFile, err := LoadFile(ctx, t.fs, name, link.Cid)
		if err != nil {
			return nil, err
		}

		previousFile.SetContents(f)
		return previousFile.Put()
	}

	ch := NewEmptyFile(t.fs, name, f)
	return ch.Put()
}

func (t *PublicTree) updateUserlandLink(name string, res base.PutResult) {
	t.userland.Add(res.ToLink(name))
	t.skeleton[name] = res.ToSkeletonInfo()
	t.h.Info.Mtime = base.Timestamp().Unix()
	t.h.Merge = nil // clear merge field in the case where we're mutating after a merge commit
}

func (t *PublicTree) removeUserlandLink(name string) {
	t.userland.Remove(name)
	delete(t.skeleton, name)
	t.h.Info.Mtime = base.Timestamp().Unix()
	t.h.Merge = nil // clear merge field in the case where we're mutating after a merge commit
}

type PublicFile struct {
	fs   base.MerkleDagFS
	name string
	cid  cid.Cid
	h    *header

	metadata *base.Metadata
	content  io.ReadCloser
}

var (
	_ fs.File   = (*PublicFile)(nil)
	_ base.Node = (*PublicFile)(nil)
)

func NewEmptyFile(fs base.MerkleDagFS, name string, content io.ReadCloser) *PublicFile {
	now := base.Timestamp().Unix()
	return &PublicFile{
		fs:      fs,
		name:    name,
		content: content,
		h: &header{
			Info: &Info{
				WNFS:  base.LatestVersion,
				Type:  base.NTFile,
				Mode:  base.ModeDefault,
				Ctime: now,
				Mtime: now,
				Size:  -1,
			},
		},
	}
}

func LoadFile(ctx context.Context, fs base.MerkleDagFS, name string, id cid.Cid) (*PublicFile, error) {

	h, err := loadHeader(ctx, fs.DagStore(), id)
	if err != nil {
		return nil, err
	}

	return fileFromHeader(ctx, fs, h, name, id)
}

func fileFromHeader(ctx context.Context, fs base.MerkleDagFS, h *header, name string, id cid.Cid) (*PublicFile, error) {
	var (
		store = fs.DagStore()
		md    *base.Metadata
		err   error
	)

	if h.Metadata != nil {
		if md, err = base.LoadMetadata(ctx, store, *h.Metadata); err != nil {
			return nil, err
		}
	}

	if h.Userland == nil {
		return nil, errors.New("header is missing 'userland' link")
	}

	return &PublicFile{
		fs:       fs,
		name:     name,
		cid:      id,
		h:        h,
		metadata: md,
	}, nil
}

func (f *PublicFile) Links() mdstore.Links       { return mdstore.NewLinks() }
func (f *PublicFile) Name() string               { return f.name }
func (f *PublicFile) Size() int64                { return f.h.Info.Size }
func (f *PublicFile) ModTime() time.Time         { return time.Unix(f.h.Info.Mtime, 0) }
func (f *PublicFile) Mode() fs.FileMode          { return fs.FileMode(f.h.Info.Mode) }
func (f *PublicFile) IsDir() bool                { return false }
func (f *PublicFile) Sys() interface{}           { return f.fs }
func (f *PublicFile) Store() base.MerkleDagFS    { return f.fs }
func (f *PublicFile) Cid() cid.Cid               { return f.cid }
func (f *PublicFile) Stat() (fs.FileInfo, error) { return f, nil }

func (f *PublicFile) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   f.name,
		Cid:    f.cid,
		Size:   f.h.Info.Size,
		IsFile: true,
		Mtime:  f.h.Info.Mtime,
	}
}

func (f *PublicFile) History(ctx context.Context, maxRevs int) ([]base.HistoryEntry, error) {
	return history(ctx, f, maxRevs)
}

func (f *PublicFile) Read(p []byte) (n int, err error) {
	f.ensureContent()
	return f.content.Read(p)
}

func (f *PublicFile) ensureContent() (err error) {
	if f.content == nil {
		ctx := f.fs.Context()
		f.content, err = f.fs.DagStore().GetFile(ctx, *f.h.Userland)
	}
	return err
}

func (f *PublicFile) Close() error {
	if closer, ok := f.content.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (f *PublicFile) SetContents(r io.ReadCloser) {
	f.content = r
}

func (f *PublicFile) Put() (base.PutResult, error) {
	store := f.fs.DagStore()

	userlandRes, err := store.PutFile(base.NewBareFile(store, "", f.content))
	if err != nil {
		return PutResult{}, fmt.Errorf("putting file %q in store: %w", f.name, err)
	}
	f.h.Userland = &userlandRes.Cid

	if f.metadata != nil {
		buf, err := base.EncodeCBOR(f.metadata)
		if err != nil {
			return PutResult{}, fmt.Errorf("encoding file %q metadata: %w", f.name, err)
		}
		metadataCid, err := store.PutBlock(buf.Bytes())
		if err != nil {
			return nil, err
		}
		f.h.Metadata = &metadataCid
	}

	// add previous reference
	if f.cid.Defined() {
		f.h.Previous = &f.cid
	}

	blk, err := f.h.encodeBlock()
	if err != nil {
		return nil, err
	}
	f.cid = blk.Cid()
	if err := f.fs.DagStore().Blockservice().Blockstore().Put(blk); err != nil {
		return nil, err
	}

	log.Debugw("wrote public file header", "name", f.name, "cid", f.cid.String())
	return PutResult{
		Cid:  f.cid,
		Size: f.h.Info.Size,
		// Metadata: *f.h.Metadata,
		Userland: *f.h.Userland,
		IsFile:   true,
	}, nil
}

func (f *PublicFile) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		Cid:      f.cid,
		Previous: f.h.Previous,
		Mtime:    f.h.Info.Mtime,
		Type:     f.h.Info.Type,
		Size:     f.h.Info.Size,
	}
}

type PutResult struct {
	Cid      cid.Cid
	Size     int64
	IsFile   bool
	Userland cid.Cid
	Metadata cid.Cid
	Skeleton base.Skeleton
}

func (r PutResult) CID() cid.Cid {
	return r.Cid
}

func (r PutResult) ToLink(name string) mdstore.Link {
	return mdstore.Link{
		Name:   name,
		Cid:    r.Cid,
		Size:   r.Size,
		IsFile: r.IsFile,
	}
}

func (r PutResult) ToSkeletonInfo() base.SkeletonInfo {
	return base.SkeletonInfo{
		Cid:         r.Cid,
		Metadata:    r.Metadata,
		Userland:    r.Userland,
		SubSkeleton: r.Skeleton,
		IsFile:      r.IsFile,
	}
}

// load a public node
func loadNode(ctx context.Context, fs base.MerkleDagFS, name string, id cid.Cid) (n base.Node, err error) {

	h, err := loadHeader(ctx, fs.DagStore(), id)
	if err != nil {
		return nil, err
	}

	if h.Info == nil {
		return nil, fmt.Errorf("not a valid wnfs header: %q", id)
	}

	switch h.Info.Type {
	case base.NTFile:
		return fileFromHeader(ctx, fs, h, name, id)
	case base.NTDataFile:
		// TODO(b5):
		blk, err := fs.DagStore().Blockservice().GetBlock(ctx, id)
		if err != nil {
			return nil, err
		}
		df := &DataFile{fs: fs, name: name, cid: id}
		return decodeDataFileBlock(df, blk)
	case base.NTDir:
		return treeFromHeader(ctx, fs, h, name, id)
	default:
		return nil, fmt.Errorf("unrecognized node type: %s", h.Info.Type)
	}
}

func loadNodeFromSkeletonInfo(ctx context.Context, fs base.MerkleDagFS, name string, info base.SkeletonInfo) (n base.Node, err error) {
	if info.IsFile {
		return LoadFile(ctx, fs, name, info.Cid)
	}
	return LoadTree(ctx, fs, name, info.Cid)
}

type DataFile struct {
	fs   base.MerkleDagFS
	name string
	cid  cid.Cid

	Info        *Info
	Metadata    *cid.Cid
	Previous    *cid.Cid // historical backpointer
	content     interface{}
	jsonContent *bytes.Buffer
}

var (
	_ fs.File   = (*DataFile)(nil)
	_ base.Node = (*DataFile)(nil)
)

func NewDataFile(fs base.MerkleDagFS, name string, content interface{}) *DataFile {
	now := base.Timestamp().Unix()
	return &DataFile{
		fs:   fs,
		name: name,
		Info: &Info{
			WNFS:  base.LatestVersion,
			Type:  base.NTDataFile,
			Mode:  base.ModeDefault,
			Mtime: now,
			Ctime: now,
			Size:  -1,
		},
		content: content,
	}
}

func LoadDataFile(ctx context.Context, fs base.MerkleDagFS, name string, id cid.Cid) (*DataFile, error) {
	df := &DataFile{
		fs:   fs,
		name: name,
		cid:  id,
	}

	blk, err := fs.DagStore().Blockservice().GetBlock(ctx, id)
	if err != nil {
		return nil, err
	}

	return decodeDataFileBlock(df, blk)
}

func decodeDataFileBlock(df *DataFile, blk blocks.Block) (*DataFile, error) {
	env := map[string]interface{}{}
	if err := cbornode.DecodeInto(blk.RawData(), &env); err != nil {
		return nil, err
	}

	// TODO (b5): links
	// nd, err := cbornode.DecodeBlock(blk)
	// if err != nil {
	// 	return nil, err
	// }
	// nd.Links()

	log.Debugw("decodeDataFileBlock", "info", env["info"], "env", env)

	if info, ok := env["info"].(map[string]interface{}); ok {
		df.Info = infoFromMap(info)
		if df.Info.WNFS == "" {
			// info MUST have wnfs key present, otherwise it's considered a bare data file
			df.Info = nil
			df.content = env
			return df, nil
		}
		df.content = env["content"]
		return df, nil
	}

	// if no info block exists, parse as a bare data file
	df.content = env

	return df, nil
}

func (df *DataFile) IsBare() bool         { return df.Info == nil }
func (df *DataFile) Links() mdstore.Links { return mdstore.NewLinks() } // TODO(b5): remove Links method?
func (df *DataFile) Name() string         { return df.name }
func (df *DataFile) Size() int64 {
	if df.Info != nil {
		return df.Info.Size
	}
	return -1
}
func (df *DataFile) ModTime() time.Time {
	if df.Info != nil {
		return time.Unix(df.Info.Mtime, 0)
	}
	return time.Time{}
}
func (df *DataFile) Mode() fs.FileMode {
	if df.Info != nil {
		return fs.FileMode(df.Info.Mode)
	}
	return fs.FileMode(0)
}
func (df *DataFile) IsDir() bool                { return false }
func (df *DataFile) Sys() interface{}           { return df.fs }
func (df *DataFile) Store() base.MerkleDagFS    { return df.fs }
func (df *DataFile) Cid() cid.Cid               { return df.cid }
func (df *DataFile) Stat() (fs.FileInfo, error) { return df, nil }

func (df *DataFile) AsLink() mdstore.Link {
	return mdstore.Link{
		Name: df.name,
		// Cid:    df.h.cid,
		// Size:   df.h.Size,
		IsFile: true,
		// Mtime:  df.metadata.UnixMeta.Mtime,
	}
}

func (df *DataFile) History(ctx context.Context, maxRevs int) ([]base.HistoryEntry, error) {
	// TODO(b5): make this base.ErrNoHistory
	return nil, fmt.Errorf("no history")
	// TODO(b5): support history
	// return history(ctx, df, maxRevs)
}

func (df *DataFile) Read(p []byte) (n int, err error) {
	df.ensureContent()
	return df.jsonContent.Read(p)
}

func (df *DataFile) ensureContent() (err error) {
	if df.jsonContent == nil {
		buf := &bytes.Buffer{}
		// TODO(b5): use faster json lib
		if err := json.NewEncoder(buf).Encode(df.content); err != nil {
			return err
		}
		df.jsonContent = buf
	}
	return nil
}

func (df *DataFile) Close() error { return nil }

func (df *DataFile) SetContents(data interface{}) {
	df.content = data
	df.jsonContent = nil
}

func (df *DataFile) Put() (result base.PutResult, err error) {
	if df.cid.Defined() {
		df.Previous = &df.cid
	}
	if df.Info == nil {
		df.Info = &Info{}
	}

	blk, err := df.encodeBlock()
	if err != nil {
		return result, err
	}
	df.cid = blk.Cid()

	if err = df.fs.DagStore().Blockservice().Blockstore().Put(blk); err != nil {
		return result, err
	}

	log.Debugw("wrote public data file", "name", df.name, "cid", df.cid.String())
	return PutResult{
		Cid:      df.cid,
		Size:     df.Info.Size,
		Userland: df.cid,
		IsFile:   true,
	}, nil
}

func (df *DataFile) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		Cid:      df.cid,
		Size:     df.Info.Size,
		Type:     df.Info.Type,
		Mtime:    df.Info.Mtime,
		Previous: df.Previous,
	}
}

func (df *DataFile) encodeBlock() (blocks.Block, error) {
	dataFile := map[string]interface{}{
		"metadata": df.Metadata,
		"previous": df.Previous,
		"content":  df.content,
	}
	if df.Info != nil {
		dataFile["info"] = df.Info.Map()
	}

	return cbornode.WrapObject(dataFile, multihash.SHA2_256, -1)
}
