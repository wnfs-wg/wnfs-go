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
	Skeleton *cid.Cid
	Userland *cid.Cid
}

var headerLinkNames = [...]string{
	base.PreviousLinkName,
	base.MergeLinkName,
	base.MetadataLinkName,
	base.UserlandLinkName,
	base.SkeletonLinkName,
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
		// TODO(b5):
		// Version: ___
	}

	for _, name := range headerLinkNames {
		if l := links.Get(name); l != nil {
			switch name {
			case base.PreviousLinkName:
				h.Previous = &l.Cid
			case base.MergeLinkName:
				h.Merge = &l.Cid
			case base.MetadataLinkName:
				h.Metadata = &l.Cid
			case base.UserlandLinkName:
				h.Userland = &l.Cid
			case base.SkeletonLinkName:
				h.Skeleton = &l.Cid
			}
		}
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

	var result base.PutResult
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
	// result, err := t.writeHeader(ctx, links)
	// if err != nil {
	// 	return nil, err
	// }
	// t.h.cid = result.CID()
	return result, nil
}

// func (t *PublicTree) writeHeader(ctx context.Context, links mdstore.Links) (base.PutResult, error) {
// 	store := t.fs.DagStore()

// 	if t.cid.Defined() {
// 		links.Add(mdstore.Link{
// 			Name: base.PreviousLinkName,
// 			Cid:  t.cid,
// 			Size: t.Size(),
// 		})
// 	}

// 	if t.merge != nil && t.merge.Defined() {
// 		links.Add(mdstore.Link{
// 			Name: base.MergeLinkName,
// 			Cid:  *t.merge,
// 			// TODO(b5): size field
// 		})
// 	}

// 	headerNode, err := store.PutNode(links)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return PutResult{
// 		Cid:      headerNode.Cid,
// 		Size:     headerNode.Size,
// 		Metadata: links.Get(base.MetadataLinkName).Cid,
// 		Userland: links.Get(base.UserlandLinkName).Cid,
// 		Skeleton: t.skeleton,
// 	}, nil
// }

func (t *PublicTree) History(path base.Path, max int) ([]base.HistoryEntry, error) {
	f, err := t.Get(path)
	if err != nil {
		return nil, err
	}

	n, ok := f.(base.Node)
	if !ok {
		return nil, fmt.Errorf("%s does not support history", path)
	}

	ctx := t.fs.Context()
	store := t.fs.DagStore()

	log := []base.HistoryEntry{
		n.AsHistoryEntry(),
	}

	prev := log[0].Previous
	for prev != nil {
		ent, err := loadHistoryEntry(ctx, store, *prev)
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

func (t *PublicTree) MergeDiverged(n base.Node) (result base.MergeResult, err error) {
	switch x := n.(type) {
	case *PublicTree:
		log.Debugw("merge trees", "local", t.h.cid, "remote", x.h.cid)
		return t.mergeDivergedTree(x)
	case *PublicFile:
		result.Type = base.MTMergeCommit
		nHist := n.AsHistoryEntry()
		t.h.Merge = &nHist.Cid
		t.metadata.UnixMeta.Mtime = base.Timestamp().Unix()
		update, err := t.Put()
		if err != nil {
			return result, err
		}

		si := update.ToSkeletonInfo()
		return base.MergeResult{
			Type:     base.MTMergeCommit,
			Cid:      si.Cid,
			Userland: si.Cid,
			Metadata: si.Metadata,
			Size:     update.ToLink("").Size,
			IsFile:   true,
		}, nil
	default:
		return result, fmt.Errorf("cannot merge node of type %T onto public directory", n)
	}
}

func (t *PublicTree) mergeDivergedTree(remote *PublicTree) (res base.MergeResult, err error) {
	for remName, remInfo := range remote.skeleton {
		localInfo, existsLocally := t.skeleton[remName]

		if !existsLocally {
			// remote has a file local is missing, add it.
			n, err := loadNodeFromSkeletonInfo(remote.fs, remName, remInfo)
			if err != nil {
				return res, err
			}

			t.skeleton[remName] = remInfo
			t.userland.Add(n.AsLink())
			continue
		}

		if localInfo.Cid.Equals(remInfo.Cid) {
			// both files are equal. no need to merge
			continue
		}

		// node exists in both trees & CIDs are inequal. merge recursively
		lcl, err := loadNodeFromSkeletonInfo(t.fs, remName, localInfo)
		if err != nil {
			return res, err
		}
		rem, err := loadNodeFromSkeletonInfo(remote.fs, remName, remInfo)
		if err != nil {
			return res, err
		}

		res, err := Merge(lcl, rem)
		if err != nil {
			return res, err
		}
		t.skeleton[remName] = res.ToSkeletonInfo()
		t.userland.Add(res.ToLink(remName))
	}

	t.h.Merge = &remote.h.cid
	t.metadata.UnixMeta.Mtime = base.Timestamp().Unix()
	update, err := t.Put()
	if err != nil {
		return res, err
	}

	si := update.ToSkeletonInfo()
	return base.MergeResult{
		Type:     base.MTMergeCommit,
		Cid:      si.Cid,
		Userland: si.Cid,
		Metadata: si.Metadata,
		Size:     update.ToLink("").Size,
		IsFile:   false,
	}, nil
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
	cid  cid.Cid
	size int64

	metadata *base.Metadata
	previous *cid.Cid
	merge    *cid.Cid
	userland cid.Cid

	content io.ReadCloser
}

var (
	_ mdstore.DagNode = (*PublicFile)(nil)
	_ fs.File         = (*PublicFile)(nil)
	_ base.Node       = (*PublicFile)(nil)
)

func NewEmptyFile(fs base.MerkleDagFS, name string, content io.ReadCloser) *PublicFile {
	return &PublicFile{
		fs:      fs,
		name:    name,
		content: content,

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
	header, err := store.GetNode(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("reading file header: %w", err)
	}

	links := header.Links()

	mdLink := links.Get(base.MetadataLinkName)
	if mdLink == nil {
		return nil, errors.New("header is missing 'metadata' link")
	}
	md, err := base.LoadMetadata(ctx, store, mdLink.Cid)
	if err != nil {
		return nil, err
	}

	userlandLink := links.Get(base.UserlandLinkName)
	if userlandLink == nil {
		return nil, errors.New("header is missing 'userland' link")
	}

	var previous *cid.Cid
	if prev := links.Get(base.PreviousLinkName); prev != nil {
		previous = &prev.Cid
	}

	var merge *cid.Cid
	if m := links.Get(base.MergeLinkName); m != nil {
		merge = &m.Cid
	}

	return &PublicFile{
		fs:   fs,
		cid:  id,
		name: name,
		size: header.Size(),

		metadata: md,
		previous: previous,
		merge:    merge,
		userland: userlandLink.Cid,
	}, nil
}

func (f *PublicFile) Links() mdstore.Links       { return mdstore.NewLinks() }
func (f *PublicFile) Name() string               { return f.name }
func (f *PublicFile) Size() int64                { return f.size }
func (f *PublicFile) ModTime() time.Time         { return time.Unix(f.metadata.UnixMeta.Mtime, 0) }
func (f *PublicFile) Mode() fs.FileMode          { return fs.FileMode(f.metadata.UnixMeta.Mode) }
func (f *PublicFile) IsDir() bool                { return false }
func (f *PublicFile) Sys() interface{}           { return f.fs }
func (f *PublicFile) Store() base.MerkleDagFS    { return f.fs }
func (f *PublicFile) Cid() cid.Cid               { return f.cid }
func (f *PublicFile) Stat() (fs.FileInfo, error) { return f, nil }

func (f *PublicFile) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   f.name,
		Cid:    f.cid,
		Size:   f.size,
		IsFile: true,
		Mtime:  f.metadata.UnixMeta.Mtime,
	}
}

func (f *PublicFile) Read(p []byte) (n int, err error) {
	f.ensureContent()
	return f.content.Read(p)
}

func (f *PublicFile) ensureContent() (err error) {
	if f.content == nil {
		ctx := f.fs.Context()
		f.content, err = f.fs.DagStore().GetFile(ctx, f.userland)
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
	links := mdstore.NewLinks(userlandRes.ToLink(base.UserlandLinkName, true))

	buf, err := base.EncodeCBOR(f.metadata)
	if err != nil {
		return PutResult{}, fmt.Errorf("encoding file %q metadata: %w", f.name, err)
	}
	metadataCid, err := store.PutBlock(buf.Bytes())
	if err != nil {
		return nil, err
	}

	links.Add(mdstore.Link{
		Name:   base.MetadataLinkName,
		Cid:    metadataCid,
		Size:   int64(buf.Len()),
		IsFile: true,
	})

	// add previous reference
	if f.cid.Defined() {
		links.Add(mdstore.Link{
			Name: base.PreviousLinkName,
			Cid:  f.cid,
		})
	}

	if f.merge != nil {
		links.Add(mdstore.Link{
			Name: base.MergeLinkName,
			Cid:  *f.merge,
		})
	}

	// write header node
	res, err := store.PutNode(links)
	if err != nil {
		return nil, err
	}

	if !f.cid.Equals(cid.Cid{}) {
		f.previous = &f.cid
	}
	f.cid = res.Cid
	log.Debugw("wrote public file header", "name", f.name, "cid", f.cid.String())
	return PutResult{
		Cid: res.Cid,
		// TODO(b5)
		// Size: f.Size(),
		Metadata: metadataCid,
		Userland: userlandRes.Cid,
		IsFile:   true,
	}, nil
}

func (f *PublicFile) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		Cid:      f.cid,
		Size:     f.size,
		Metadata: f.metadata,
		Previous: f.previous,
	}
}

func (f *PublicFile) MergeDiverged(b base.Node) (result base.MergeResult, err error) {
	result.Type = base.MTMergeCommit
	bHist := b.AsHistoryEntry()
	f.merge = &bHist.Cid
	f.metadata.UnixMeta.Mtime = base.Timestamp().Unix()
	if err := f.ensureContent(); err != nil {
		return result, err
	}

	update, err := f.Put()
	if err != nil {
		return result, err
	}

	si := update.ToSkeletonInfo()
	return base.MergeResult{
		Type:     base.MTMergeCommit,
		Cid:      si.Cid,
		Userland: si.Cid,
		Metadata: si.Metadata,
		Size:     update.ToLink("").Size,
		IsFile:   true,
	}, nil
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

func Merge(a, b base.Node) (result base.MergeResult, err error) {
	var (
		aCur, bCur   = a, b
		aHist, bHist = a.AsHistoryEntry(), b.AsHistoryEntry()
		aGen, bGen   = 0, 0
	)

	// check for equality first
	if aHist.Cid.Equals(bHist.Cid) {
		return base.MergeResult{
			Type: base.MTInSync,
			Cid:  aHist.Cid,
			// Userland: aHist.Userland,
			// Metadata: bHist.Metadata,
			Size:   aHist.Size,
			IsFile: aHist.Metadata.IsFile,
		}, nil
	}

	afs, err := base.NodeFS(a)
	if err != nil {
		return result, err
	}
	bfs, err := base.NodeFS(b)
	if err != nil {
		return result, err
	}

	for {
		bCur = b
		bGen = 0
		bHist = b.AsHistoryEntry()
		for {
			if aHist.Cid.Equals(bHist.Cid) {
				if aGen == 0 && bGen > 0 {
					// fast-forward
					bHist = b.AsHistoryEntry()
					return base.MergeResult{
						Type: base.MTFastForward,
						// TODO(b5):
						// 	Userland: si.Cid,
						// 	Metadata: si.Metadata,
						Cid:    bHist.Cid,
						Size:   bHist.Size,
						IsFile: bHist.Metadata.IsFile,
					}, nil
				} else if aGen > 0 && bGen == 0 {
					result.Type = base.MTLocalAhead
					aHist := a.AsHistoryEntry()
					return base.MergeResult{
						Type:   base.MTLocalAhead,
						Cid:    aHist.Cid,
						Size:   aHist.Size,
						IsFile: aHist.Metadata.IsFile,
					}, nil
				} else {
					// both local & remote are greater than zero, have diverged
					if aGen > bGen || (aGen == bGen && base.LessCID(aHist.Cid, bHist.Cid)) {
						return a.MergeDiverged(b)
					}
					return b.MergeDiverged(a)
				}
			}

			if bHist.Previous == nil {
				break
			}
			name, err := base.Filename(bCur)
			if err != nil {
				return result, err
			}
			bCur, err = loadNode(bfs, name, *bHist.Previous)
			if err != nil {
				return result, err
			}
			bHist = bCur.AsHistoryEntry()
			bGen++
		}

		if aHist.Previous == nil {
			break
		}
		name, err := base.Filename(aCur)
		if err != nil {
			return result, err
		}
		aCur, err = loadNode(afs, name, *aHist.Previous)
		if err != nil {
			return result, err
		}
		aHist = aCur.AsHistoryEntry()
		aGen++
	}

	// no common history, merge based on heigh & alpha-sorted-cid
	if aGen > bGen || (aGen == bGen && base.LessCID(aHist.Cid, bHist.Cid)) {
		return a.MergeDiverged(b)
	}
	return b.MergeDiverged(a)
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
