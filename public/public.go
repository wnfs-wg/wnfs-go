package public

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	cid "github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log"
	"github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/mdstore"
)

var log = golog.Logger("wnfs")

type header struct {
	cid cid.Cid

	Size    int64
	Version base.SemVer

	Previous *cid.Cid // historical backpointer
	Merge    *cid.Cid // if this version is a merge, will be populated
	Metadata *cid.Cid
	Skeleton *cid.Cid // only present on directories
	Userland *cid.Cid
}

func loadHeader(s base.MerkleDagFS, id cid.Cid) (*header, error) {
	n, err := s.DagStore().GetNode(s.Context(), id)
	if err != nil {
		return nil, fmt.Errorf("loading header node %s:\n%w", id, err)
	}
	links := n.Links()

	h := &header{
		cid:  id,
		Size: n.Size(),
		// TODO(b5)
		// Version: ___
	}

	if l := links.Get(base.PreviousLinkName); l != nil {
		h.Previous = &l.Cid
	}
	if l := links.Get(base.MergeLinkName); l != nil {
		h.Merge = &l.Cid
	}
	if l := links.Get(base.MetadataLinkName); l != nil {
		h.Metadata = &l.Cid
	}
	if l := links.Get(base.SkeletonLinkName); l != nil {
		h.Skeleton = &l.Cid
	}
	if l := links.Get(base.UserlandLinkName); l != nil {
		h.Userland = &l.Cid
	}

	return h, nil
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

func (h *header) write(fs base.MerkleDagFS) error {
	res, err := fs.DagStore().PutNode(h.links())
	if err != nil {
		return err
	}

	h.cid = res.Cid
	h.Size = res.Size
	return nil
}

type PublicTree struct {
	fs   base.MerkleDagFS // embed a reference to store this tree is associated with
	name string           // directory name, used while linking
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
	return &PublicTree{
		fs:   fs,
		name: name,
		h:    &header{},

		userland: mdstore.NewLinks(),
		metadata: &base.Metadata{
			UnixMeta: base.NewUnixMeta(false),
			Version:  base.LatestVersion,
		},
		skeleton: base.Skeleton{},
	}
}

func LoadTreeFromCID(fs base.MerkleDagFS, name string, id cid.Cid) (*PublicTree, error) {
	ctx := fs.Context()
	log.Debugw("loadTreeFromCID", "name", name, "cid", id)
	store := fs.DagStore()

	h, err := loadHeader(fs, id)
	if err != nil {
		return nil, err
	}

	md, err := h.loadMetadata(fs)
	if err != nil {
		return nil, err
	}
	if md.IsFile {
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
		h:    h,

		metadata: md,
		skeleton: sk,
		userland: userland.Links(),
	}, nil
}

func (t *PublicTree) Links() mdstore.Links       { return t.userland }
func (t *PublicTree) Raw() []byte                { return nil }
func (t *PublicTree) Name() string               { return t.name }
func (t *PublicTree) Size() int64                { return t.h.Size }
func (t *PublicTree) ModTime() time.Time         { return time.Unix(t.metadata.UnixMeta.Mtime, 0) }
func (t *PublicTree) Mode() fs.FileMode          { return fs.FileMode(t.metadata.UnixMeta.Mode) }
func (t *PublicTree) IsDir() bool                { return true }
func (t *PublicTree) Sys() interface{}           { return t.fs }
func (t *PublicTree) Store() base.MerkleDagFS    { return t.fs }
func (t *PublicTree) Cid() cid.Cid               { return t.h.cid }
func (t *PublicTree) Stat() (fs.FileInfo, error) { return t, nil }

func (t *PublicTree) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   t.name,
		Cid:    t.h.cid,
		Size:   t.h.Size,
		IsFile: false,
		Mtime:  t.metadata.UnixMeta.Mtime,
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
	head, tail := path.Shift()
	if head == "" {
		return t, nil
	}

	link := t.userland.Get(head)
	if link == nil {
		return nil, base.ErrNotFound
	}

	if tail != nil {
		ch, err := LoadTreeFromCID(t.fs, head, link.Cid)
		if err != nil {
			return nil, err
		}

		// recurse
		return ch.Get(tail)
	}

	if t.skeleton[head].IsFile {
		return LoadFileFromCID(t.fs, link.Name, link.Cid)
	}

	return LoadTreeFromCID(t.fs, link.Name, link.Cid)
}

func (t *PublicTree) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		Metadata: t.metadata,
		Cid:      t.h.cid,
		Previous: t.h.Previous,
		Size:     t.h.Size,
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
		child, err := LoadTreeFromCID(t.fs, head, link.Cid)
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

	metaBuf, err := base.EncodeCBOR(t.metadata)
	if err != nil {
		return nil, err
	}
	id, err := store.PutBlock(metaBuf.Bytes())
	if err != nil {
		return nil, err
	}
	t.h.Metadata = &id

	skf, err := t.skeleton.CBORFile()
	if err != nil {
		return nil, err
	}
	res, err := store.PutFile(skf)
	if err != nil {
		return nil, err
	}
	t.h.Skeleton = &res.Cid

	if t.h.cid.Defined() {
		// need to copy CID, as we're about to alter it's value
		id, _ := cid.Parse(t.h.cid)
		t.h.Previous = &id
	}

	if err = t.h.write(t.fs); err != nil {
		return nil, err
	}
	log.Debugw("wrote public tree", "name", t.name, "cid", t.h.cid.String(), "userlandLinkCount", t.userland.Len(), "size", t.h.Size, "prev", t.h.Previous)

	return PutResult{
		Cid:      t.h.cid,
		Size:     t.h.Size,
		Metadata: *t.h.Metadata,
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
	node, err := store.GetNode(ctx, id)
	if err != nil {
		return base.HistoryEntry{}, err
	}

	links := node.Links()
	ent := base.HistoryEntry{
		Cid:  id,
		Size: node.Size(),
	}
	if mdLnk := links.Get(base.MetadataLinkName); mdLnk != nil {
		ent.Metadata, err = base.LoadMetadata(ctx, store, mdLnk.Cid)
	}
	if prvLnk := links.Get(base.PreviousLinkName); prvLnk != nil {
		ent.Previous = &prvLnk.Cid
	}
	return ent, err
}

func (t *PublicTree) getOrCreateDirectChildTree(name string) (*PublicTree, error) {
	link := t.userland.Get(name)
	if link == nil {
		return NewEmptyTree(t.fs, name), nil
	}
	return LoadTreeFromCID(t.fs, name, link.Cid)
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
		tree, err = LoadTreeFromCID(t.fs, name, link.Cid)
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
	if link := t.userland.Get(name); link != nil {
		previousFile, err := LoadFileFromCID(t.fs, name, link.Cid)
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
	t.metadata.UnixMeta.Mtime = base.Timestamp().Unix()
	t.h.Merge = nil // clear merge field in the case where we're mutating after a merge commit
}

func (t *PublicTree) removeUserlandLink(name string) {
	t.userland.Remove(name)
	delete(t.skeleton, name)
	t.metadata.UnixMeta.Mtime = base.Timestamp().Unix()
	t.h.Merge = nil // clear merge field in the case where we're mutating after a merge commit
}

type PublicFile struct {
	fs   base.MerkleDagFS
	name string
	h    *header

	metadata *base.Metadata
	content  io.ReadCloser
}

var (
	_ fs.File   = (*PublicFile)(nil)
	_ base.Node = (*PublicFile)(nil)
)

func NewEmptyFile(fs base.MerkleDagFS, name string, content io.ReadCloser) *PublicFile {
	return &PublicFile{
		fs:      fs,
		name:    name,
		content: content,
		h:       &header{},

		metadata: &base.Metadata{
			UnixMeta: base.NewUnixMeta(true),
			IsFile:   true,
			Version:  base.LatestVersion,
		},
	}
}

func LoadFileFromCID(fs base.MerkleDagFS, name string, id cid.Cid) (*PublicFile, error) {
	store := fs.DagStore()
	ctx := fs.Context()
	h, err := loadHeader(fs, id)
	if err != nil {
		return nil, err
	}
	// header, err := store.GetNode(ctx, id)
	// if err != nil {
	// 	return nil, fmt.Errorf("reading file header: %w", err)
	// }
	// links := header.Links()

	// mdLink := links.Get(base.MetadataLinkName)
	if h.Metadata == nil {
		return nil, errors.New("header is missing 'metadata' link")
	}
	md, err := base.LoadMetadata(ctx, store, *h.Metadata)
	if err != nil {
		return nil, err
	}

	if h.Userland == nil {
		return nil, errors.New("header is missing 'userland' link")
	}

	return &PublicFile{
		fs:   fs,
		name: name,
		h:    h,

		metadata: md,
	}, nil
}

func (f *PublicFile) Links() mdstore.Links       { return mdstore.NewLinks() }
func (f *PublicFile) Name() string               { return f.name }
func (f *PublicFile) Size() int64                { return f.h.Size }
func (f *PublicFile) ModTime() time.Time         { return time.Unix(f.metadata.UnixMeta.Mtime, 0) }
func (f *PublicFile) Mode() fs.FileMode          { return fs.FileMode(f.metadata.UnixMeta.Mode) }
func (f *PublicFile) IsDir() bool                { return false }
func (f *PublicFile) Sys() interface{}           { return f.fs }
func (f *PublicFile) Store() base.MerkleDagFS    { return f.fs }
func (f *PublicFile) Cid() cid.Cid               { return f.h.cid }
func (f *PublicFile) Stat() (fs.FileInfo, error) { return f, nil }

func (f *PublicFile) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   f.name,
		Cid:    f.h.cid,
		Size:   f.h.Size,
		IsFile: true,
		Mtime:  f.metadata.UnixMeta.Mtime,
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

	buf, err := base.EncodeCBOR(f.metadata)
	if err != nil {
		return PutResult{}, fmt.Errorf("encoding file %q metadata: %w", f.name, err)
	}
	metadataCid, err := store.PutBlock(buf.Bytes())
	if err != nil {
		return nil, err
	}

	f.h.Metadata = &metadataCid
	// add previous reference
	if f.h.cid.Defined() {
		f.h.Previous = &f.h.cid
	}

	if err = f.h.write(f.fs); err != nil {
		return nil, err
	}

	log.Debugw("wrote public file header", "name", f.name, "cid", f.h.cid.String())
	return PutResult{
		Cid:      f.h.cid,
		Size:     f.h.Size,
		Metadata: *f.h.Metadata,
		Userland: *f.h.Userland,
		IsFile:   true,
	}, nil
}

func (f *PublicFile) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		Cid:      f.h.cid,
		Size:     f.h.Size,
		Metadata: f.metadata,
		Previous: f.h.Previous,
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
func loadNode(fs base.MerkleDagFS, name string, id cid.Cid) (n base.Node, err error) {
	ctx := fs.Context()
	st := fs.DagStore()
	header, err := st.GetNode(ctx, id)
	if err != nil {
		return n, err
	}
	metaLink := header.Links().Get(base.MetadataLinkName)
	if metaLink == nil {
		return nil, fmt.Errorf("cid %s does not point to a well-formed public header block: missing meta link", id.String())
	}

	md, err := base.LoadMetadata(ctx, st, metaLink.Cid)
	if err != nil {
		return n, fmt.Errorf("error loading meta at cid %s: %w", metaLink.Cid, err)
	}
	if md.IsFile {
		return LoadFileFromCID(fs, name, id)
	}

	return LoadTreeFromCID(fs, name, id)
}

func loadNodeFromSkeletonInfo(fs base.MerkleDagFS, name string, info base.SkeletonInfo) (n base.Node, err error) {
	if info.IsFile {
		return LoadFileFromCID(fs, name, info.Cid)
	}
	return LoadTreeFromCID(fs, name, info.Cid)
}
