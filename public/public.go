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

type PublicTree struct {
	fs   base.MerkleDagFS // embed a reference to store this tree is associated with
	name string           // directory name, used while linking
	cid  cid.Cid
	size int64

	// header data
	metadata *base.Metadata
	skeleton base.Skeleton
	previous *cid.Cid
	merge    *cid.Cid      // if this is a merge commit, will be populated
	userland mdstore.Links // links to files are stored in "userland" header key
}

var (
	_ mdstore.DagNode     = (*PublicTree)(nil)
	_ base.Tree           = (*PublicTree)(nil)
	_ base.SkeletonSource = (*PublicTree)(nil)
	_ fs.File             = (*PublicTree)(nil)
	_ fs.ReadDirFile      = (*PublicTree)(nil)
)

func NewEmptyTree(fs base.MerkleDagFS, name string) *PublicTree {
	return &PublicTree{
		fs:   fs,
		name: name,

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
	header, err := store.GetNode(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("loading header node %s:\n%w", id, err)
	}

	links := header.Links()

	mdLnk := links.Get(base.MetadataLinkName)
	if mdLnk == nil {
		return nil, fmt.Errorf("header is missing %s link", base.MetadataLinkName)
	}
	md, err := base.LoadMetadata(ctx, store, mdLnk.Cid)
	if err != nil {
		return nil, fmt.Errorf("loading %s data %s:\n%w", base.MetadataLinkName, mdLnk.Cid, err)
	}
	if md.IsFile {
		return nil, fmt.Errorf("expected file to be a tree")
	}

	skLnk := links.Get(base.SkeletonLinkName)
	if skLnk == nil {
		return nil, fmt.Errorf("header is missing %s link", base.SkeletonLinkName)
	}
	sk, err := base.LoadSkeleton(ctx, store, skLnk.Cid)
	if err != nil {
		return nil, fmt.Errorf("loading %s data %s:\n%w", base.SkeletonLinkName, skLnk.Cid, err)
	}

	var previous *cid.Cid
	if prev := links.Get(base.PreviousLinkName); prev != nil {
		previous = &prev.Cid
	}
	var merge *cid.Cid
	if m := links.Get(base.MergeLinkName); m != nil {
		merge = &m.Cid
	}

	userlandLnk := links.Get(base.UserlandLinkName)
	if userlandLnk == nil {
		return nil, fmt.Errorf("header is missing %s link", base.UserlandLinkName)
	}
	userland, err := store.GetNode(ctx, userlandLnk.Cid)
	if err != nil {
		return nil, fmt.Errorf("loading %s data %s:\n%w", base.UserlandLinkName, userlandLnk.Cid, err)
	}

	return &PublicTree{
		fs:   fs,
		name: name,
		size: header.Size(),

		cid:      header.Cid(),
		metadata: md,
		skeleton: sk,
		previous: previous,
		merge:    merge,
		userland: userland.Links(),
	}, nil
}

func (t *PublicTree) Name() string         { return t.name }
func (t *PublicTree) Cid() cid.Cid         { return t.cid }
func (t *PublicTree) Size() int64          { return t.size }
func (t *PublicTree) Links() mdstore.Links { return t.userland }
func (t *PublicTree) Raw() []byte          { return nil }
func (t *PublicTree) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   t.name,
		Cid:    t.cid,
		Size:   t.size,
		IsFile: false,
		Mtime:  t.metadata.UnixMeta.Mtime,
	}
}

func (t *PublicTree) Stat() (fs.FileInfo, error) {
	return base.NewFSFileInfo(
		t.name,
		t.size,
		// TODO (b5):
		// mode:  t.metadata.UnixMeta.Mode,
		fs.ModeDir,
		time.Unix(t.metadata.UnixMeta.Mtime, 0),
		t.fs,
	), nil
}

func (t *PublicTree) IsDir() bool { return true }

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
		Cid:      t.cid,
		Previous: t.previous,
		Size:     t.size,
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
	ctx := t.fs.Context()
	userlandResult, err := store.PutNode(t.userland)
	if err != nil {
		return nil, err
	}

	links := mdstore.NewLinks(userlandResult.ToLink(base.UserlandLinkName, false))

	metaBuf, err := base.EncodeCBOR(t.metadata)
	if err != nil {
		return nil, err
	}
	id, err := store.PutBlock(metaBuf.Bytes())
	if err != nil {
		return nil, err
	}
	links.Add(mdstore.Link{
		Name: base.MetadataLinkName,
		Cid:  id,
		Size: int64(metaBuf.Len()),
	})

	skf, err := t.skeleton.CBORFile()
	if err != nil {
		return nil, err
	}
	res, err := store.PutFile(skf)
	if err != nil {
		return nil, err
	}
	links.Add(res.ToLink(base.SkeletonLinkName, true))

	result, err := t.writeHeader(ctx, links)
	if err != nil {
		return nil, err
	}

	if t.cid.Defined() {
		// need to copy CID, as we're about to alter it's value
		id, _ := cid.Parse(t.cid)
		t.previous = &id
	}
	t.cid = result.CID()
	log.Debugw("wrote public tree", "name", t.name, "cid", t.cid.String(), "userlandLinkCount", t.userland.Len(), "size", t.size, "prev", t.previous)
	return result, nil
}

func (t *PublicTree) writeHeader(ctx context.Context, links mdstore.Links) (base.PutResult, error) {
	store := t.fs.DagStore()

	if t.cid.Defined() {
		links.Add(mdstore.Link{
			Name: base.PreviousLinkName,
			Cid:  t.cid,
			Size: t.Size(),
		})
	}

	if t.merge != nil && t.merge.Defined() {
		links.Add(mdstore.Link{
			Name: base.MergeLinkName,
			Cid:  *t.merge,
			// TODO(b5): size field
		})
	}

	headerNode, err := store.PutNode(links)
	if err != nil {
		return nil, err
	}

	return PutResult{
		Cid:      headerNode.Cid,
		Size:     headerNode.Size,
		Metadata: links.Get(base.MetadataLinkName).Cid,
		Userland: links.Get(base.UserlandLinkName).Cid,
		Skeleton: t.skeleton,
	}, nil
}

func (t *PublicTree) Merge(remote base.Node) (result base.MergeResult, err error) {
	switch x := remote.(type) {
	case *PublicFile:
		// need to pick either the file or the directory based on version history
		return result, fmt.Errorf("unfinished: merging public file onto directory")
	case *PublicTree:
		log.Debugw("merge trees", "local", t.cid, "remote", x.cid)
		return t.mergeTree(x)
	default:
		return result, fmt.Errorf("root node mismatch. expected Public Tree, got: %T", remote)
	}
}

func (t *PublicTree) mergeTree(remote *PublicTree) (result base.MergeResult, err error) {
	log.Debugw("comparing tree histories", "name", t.name)
	// fast-check for equality
	if t.cid.Equals(remote.cid) {
		return base.MergeResult{
			Type: base.MTInSync,
			Cid:  t.cid,
			// Userland: t.,
			// Metadata: t,
			Size:   t.size,
			IsFile: false,
		}, nil
	}

	lcl := t
	lclGen, remGen := 0, 0
	for {
		rem := remote
		remGen = 0

		for {
			if lcl.cid.Equals(rem.cid) {
				log.Debugw("found common version", "diverged_at_cid", lcl.cid, "local_generation", lclGen, "remote_generation", remGen)

				if lclGen == 0 && remGen > 0 {
					result.Type = base.MTFastForward
					// TODO(b5): copy remote history into local tree?
					*t = *remote
					return result, nil
				} else if lclGen > 0 && remGen == 0 {
					result.Type = base.MTLocalAhead
					return result, nil
				} else {
					// both local & remote are greater than zero, have diverged
					result.Type = base.MTMergeCommit
					return t.mergeDivergedTree(remote)
				}
			}

			if rem.previous == nil {
				break
			}
			if rem, err = LoadTreeFromCID(rem.fs, rem.name, *rem.previous); err != nil {
				return result, err
			}
			remGen++
		}

		if lcl.previous == nil {
			// TODO (b5): instead of erroring out here, just take the longer history
			// and create a merge commit
			return result, base.ErrNoCommonHistory
		}
		// step back one generation
		if lcl, err = LoadTreeFromCID(lcl.fs, lcl.name, *lcl.previous); err != nil {
			return result, err
		}
		lclGen++
	}
}

func (t *PublicTree) mergeDivergedTree(remote *PublicTree) (res base.MergeResult, err error) {
	for remName, remInfo := range remote.skeleton {
		localInfo, existsLocally := t.skeleton[remName]

		if !existsLocally {
			// remote has a file local is missing, add it.
			n, err := loadNodeFromInfo(remote.fs, remName, remInfo)
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
		lcl, err := loadNodeFromInfo(t.fs, remName, localInfo)
		if err != nil {
			return res, err
		}
		rem, err := loadNodeFromInfo(remote.fs, remName, remInfo)
		if err != nil {
			return res, err
		}

		res, err := lcl.Merge(rem)
		if err != nil {
			return res, err
		}
		t.skeleton[remName] = res.ToSkeletonInfo()
		t.userland.Add(res.ToLink(remName))
	}

	t.merge = &remote.cid
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
	t.merge = nil // clear merge field in the case where we're mutating after a merge commit
}

func (t *PublicTree) removeUserlandLink(name string) {
	t.userland.Remove(name)
	delete(t.skeleton, name)
	t.metadata.UnixMeta.Mtime = base.Timestamp().Unix()
	t.merge = nil // clear merge field in the case where we're mutating after a merge commit
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

func (f *PublicFile) Name() string         { return f.name }
func (f *PublicFile) Cid() cid.Cid         { return f.cid }
func (f *PublicFile) Size() int64          { return f.size }
func (f *PublicFile) Links() mdstore.Links { return mdstore.NewLinks() }
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

func (f *PublicFile) Stat() (fs.FileInfo, error) {
	return base.NewFSFileInfo(
		f.name,
		f.size,
		fs.FileMode(f.metadata.UnixMeta.Mode),
		time.Unix(f.metadata.UnixMeta.Mtime, 0),
		f.fs,
	), nil
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
	log.Debugw("wrote public file", "name", f.name, "cid", f.cid.String())
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

func (f *PublicFile) Merge(remote base.Node) (result base.MergeResult, err error) {
	switch x := remote.(type) {
	case *PublicFile:
		// need to pick either the file or the directory based on version history
		log.Debugw("merge files", "local", f.cid, "remote", x.cid)
		return f.mergeFile(x)
	case *PublicTree:
		return result, fmt.Errorf("unfinished: merging public tree onto file")
	default:
		return result, fmt.Errorf("root node mismatch. expected Public Tree, got: %T", remote)
	}
}

func (f *PublicFile) mergeFile(remote *PublicFile) (result base.MergeResult, err error) {
	log.Debugw("mergeFile", "local_name", f.name, "local_cid", f.cid, "remote_name", remote.name, "remote_cid", remote.cid)
	result.IsFile = true
	// fast-check for equality
	if f.cid.Equals(remote.cid) {
		return base.MergeResult{
			Type: base.MTInSync,
			Cid:  f.cid,
			// Userland: f.userland,
			// Metadata: f.metadata,
			Size:   f.size,
			IsFile: true,
		}, nil
	}

	lcl := f
	lclGen, remGen := 0, 0
	for {
		rem := remote
		remGen = 0

		for {
			if lcl.cid.Equals(rem.cid) {
				log.Debugw("found common version", "diverged_at_cid", lcl.cid, "local_generation", lclGen, "remote_generation", remGen)

				if lclGen == 0 && remGen > 0 {
					result.Type = base.MTFastForward
					result.Cid = remote.cid
					// TODO(b5): copy remote history into local tree?
					*f = *remote
					return result, nil
				} else if lclGen > 0 && remGen == 0 {
					result.Type = base.MTLocalAhead
					result.Cid = f.cid
					return result, nil
				} else {
					// both local & remote are greater than zero, have diverged
					result.Type = base.MTMergeCommit
					f.merge = &remote.cid
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
			}

			if rem.previous == nil {
				break
			}
			if rem, err = LoadFileFromCID(rem.fs, rem.name, *rem.previous); err != nil {
				return result, err
			}
			remGen++
		}

		if lcl.previous == nil {
			// TODO (b5): instead of erroring out here, just take the longer history
			// and create a merge commit
			return result, base.ErrNoCommonHistory
		}
		// step back one generation
		if lcl, err = LoadFileFromCID(lcl.fs, lcl.name, *lcl.previous); err != nil {
			return result, err
		}
		lclGen++
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

func loadNodeFromInfo(fs base.MerkleDagFS, name string, info base.SkeletonInfo) (n base.Node, err error) {
	if info.IsFile {
		return LoadFileFromCID(fs, name, info.Cid)
	}
	return LoadTreeFromCID(fs, name, info.Cid)
}
