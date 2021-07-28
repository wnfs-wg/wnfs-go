package wnfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

type PublicTree struct {
	fs   merkleDagFS // embed a reference to store this tree is associated with
	name string      // directory name, used while linking
	cid  cid.Cid
	size int64

	// header data
	metadata map[string]interface{}
	// metadata *Metadata
	skeleton Skeleton
	previous *cid.Cid
	userland mdstore.Links // links to files are stored in "userland" header key
}

func newEmptyPublicTree(fs merkleDagFS, name string) *PublicTree {
	return &PublicTree{
		fs:   fs,
		name: name,

		userland: mdstore.NewLinks(),
		metadata: map[string]interface{}{
			"unixMeta": NewUnixMeta(false),
			"version":  LatestVersion,
		},
		skeleton: Skeleton{},
	}
}

func loadTreeFromCID(fs merkleDagFS, name string, id cid.Cid) (*PublicTree, error) {
	log.Debugw("loadTreeFromCID", "name", name, "cid", id)
	store := fs.DagStore()
	header, err := store.GetNode(id)
	if err != nil {
		return nil, fmt.Errorf("loading header node %s:\n%w", id, err)
	}

	links := header.Links()

	// mdLnk := links.Get(metadataLinkName)
	// if mdLnk == nil {
	// 	return nil, fmt.Errorf("header is missing %s link", metadataLinkName)
	// }
	// md, err := loadMetadata(store, mdLnk.Cid)
	// if err != nil {
	// 	return nil, fmt.Errorf("loading %s data %s:\n%w", metadataLinkName, mdLnk.Cid, err)
	// }
	// if md.IsFile {
	// 	return nil, fmt.Errorf("expected file to be a tree")
	// }

	skLnk := links.Get(skeletonLinkName)
	if skLnk == nil {
		return nil, fmt.Errorf("header is missing %s link", skeletonLinkName)
	}
	sk, err := loadSkeleton(store, skLnk.Cid)
	if err != nil {
		return nil, fmt.Errorf("loading %s data %s:\n%w", skeletonLinkName, skLnk.Cid, err)
	}

	var previous *cid.Cid
	if prev := links.Get(previousLinkName); prev != nil {
		previous = &prev.Cid
	}

	userlandLnk := links.Get(userlandLinkName)
	if userlandLnk == nil {
		return nil, fmt.Errorf("header is missing %s link", userlandLinkName)
	}
	userland, err := store.GetNode(userlandLnk.Cid)
	if err != nil {
		return nil, fmt.Errorf("loading %s data %s:\n%w", userlandLinkName, userlandLnk.Cid, err)
	}

	return &PublicTree{
		fs:   fs,
		name: name,
		size: header.Size(),

		cid: header.Cid(),
		// metadata: md,
		skeleton: sk,
		previous: previous,
		userland: userland.Links(),
	}, nil
}

var (
	_ mdstore.DagNode = (*PublicTree)(nil)
	_ Tree            = (*PublicTree)(nil)
	_ fs.File         = (*PublicTree)(nil)
	_ fs.ReadDirFile  = (*PublicTree)(nil)
)

func (t *PublicTree) IsFile() bool         { return false }
func (t *PublicTree) Name() string         { return t.name }
func (t *PublicTree) Cid() cid.Cid         { return t.cid }
func (t *PublicTree) Size() int64          { return t.size }
func (t *PublicTree) Links() mdstore.Links { return t.userland }
func (t *PublicTree) Raw() []byte          { return nil }

func (t *PublicTree) Stat() (fs.FileInfo, error) {
	return &fsFileInfo{
		name: t.name,
		size: t.size,
		// TODO (b5):
		// mode:  t.metadata.UnixMeta.Mode,
		mode:  fs.ModeDir,
		mtime: time.Unix(int64(t.metadata["unixMeta"].(map[string]interface{})["mtime"].(int)), 0),
		sys:   t.fs,
	}, nil
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
		entries = append(entries, fsDirEntry{
			name:   link.Name,
			isFile: link.IsFile,
		})

		if i == n {
			break
		}
	}
	return entries, nil
}

func (t *PublicTree) Header() TreeHeader {
	// TODO(b5): finish
	return &treeInfo{
		// metadata: *t.metadata,
	}
}

func (t *PublicTree) Get(path Path) (fs.File, error) {
	head, tail := path.Shift()
	if head == "" {
		return t, nil
	}

	link := t.userland.Get(head)
	if link == nil {
		return nil, ErrNotFound
	}

	if tail != nil {
		ch, err := loadTreeFromCID(t.fs, head, link.Cid)
		if err != nil {
			return nil, fmt.Errorf("loading tree %q %s:\n%s", head, link.Cid, err)
		}

		// recurse
		return ch.Get(tail)
	}

	if t.skeleton[head].IsFile {
		return loadPublicFileFromCID(t.fs, link.Cid, link.Name)
	}

	return loadTreeFromCID(t.fs, link.Name, link.Cid)
}

func (t *PublicTree) Mkdir(path Path) (res putResult, err error) {
	if len(path) < 1 {
		return res, errors.New("invalid path: empty")
	}

	head, tail := path.Shift()
	childDir, err := t.getOrCreateDirectChildTree(head)
	if err != nil {
		return putResult{}, err
	}

	if tail == nil {
		res, err = childDir.Put()
		if err != nil {
			return putResult{}, err
		}
	} else {
		res, err = t.Mkdir(tail)
		if err != nil {
			return putResult{}, err
		}
	}

	t.updateUserlandLink(head, res)
	return t.Put()
}

func (t *PublicTree) Add(path Path, f fs.File) (res putResult, err error) {
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

func (t *PublicTree) Copy(path Path, srcPathStr string, srcFS fs.FS) (res putResult, err error) {
	log.Debugw("PublicTree.copy", "path", path, "srcPath", srcPathStr)
	if len(path) == 0 {
		return res, errors.New("invalid path: empty")
	}

	head, tail := path.Shift()
	if tail == nil {
		f, err := srcFS.Open(srcPathStr)
		if err != nil {
			return putResult{}, err
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

func (t *PublicTree) Rm(path Path) (putResult, error) {
	head, tail := path.Shift()
	if head == "" {
		return putResult{}, fmt.Errorf("invalid path: empty")
	}

	if tail == nil {
		t.removeUserlandLink(head)
	} else {
		link := t.userland.Get(head)
		if link == nil {
			return putResult{}, ErrNotFound
		}
		child, err := loadTreeFromCID(t.fs, head, link.Cid)
		if err != nil {
			return putResult{}, err
		}

		// recurse
		res, err := child.Rm(tail)
		if err != nil {
			return putResult{}, err
		}
		t.updateUserlandLink(head, res)
	}

	// contents of tree have changed, write an update.
	// TODO(b5) - pretty sure this is a bug if multiple writes are batched in the
	// same "publish" / transaction. Write advances the previous / current CID,
	// so if the same directory is mutated multiple times before the next snapshot
	// we'll have intermediate states as the "previous" pointer
	return t.Put()
}

func (t *PublicTree) Put() (putResult, error) {
	store := t.fs.DagStore()
	userlandResult, err := store.PutNode(t.userland)
	if err != nil {
		return putResult{}, err
	}

	links := mdstore.NewLinks(userlandResult.ToLink(userlandLinkName, false))

	for _, filer := range []CBORFiler{
		// t.metadata,
		t.skeleton,
	} {
		file, err := filer.CBORFile(nil)
		if err != nil {
			return putResult{}, err
		}

		linkName, err := filename(file)
		if err != nil {
			return putResult{}, err
		}

		res, err := store.PutFile(file)
		if err != nil {
			return putResult{}, err
		}
		log.Debugw("skeleton link", "cid", res.Cid, "size", res.Size)

		links.Add(res.ToLink(linkName, true))
	}

	result, err := t.writeHeader(links)
	if err != nil {
		return putResult{}, err
	}

	t.cid = result.Cid
	log.Debugw("wrote public tree", "name", t.name, "cid", t.cid.String(), "userlandLinkCount", t.userland.Len(), "size", t.size)
	return result, nil
}

func (t *PublicTree) writeHeader(links mdstore.Links) (putResult, error) {
	store := t.fs.DagStore()

	if !t.cid.Equals(cid.Cid{}) {
		// TODO(b5): incorrect
		n, err := store.GetNode(t.cid)
		if err != nil {
			return putResult{}, err
		}

		links.Add(mdstore.Link{
			Name: previousLinkName,
			Cid:  t.cid,
			Size: n.Size(),
		})
	}

	headerNode, err := store.PutNodeWithData(map[string]interface{}{
		metadataLinkName: t.metadata,
	}, links)
	if err != nil {
		return putResult{}, err
	}

	return putResult{
		Cid:  headerNode.Cid,
		Size: headerNode.Size,
		// Metadata: links.Get(metadataLinkName).Cid,
		Userland: links.Get(userlandLinkName).Cid,
		Skeleton: t.skeleton,
	}, nil
}

func (t *PublicTree) getOrCreateDirectChildTree(name string) (*PublicTree, error) {
	link := t.userland.Get(name)
	if link == nil {
		return newEmptyPublicTree(t.fs, name), nil
	}

	return loadTreeFromCID(t.fs, name, link.Cid)
}

func (t *PublicTree) createOrUpdateChild(srcPathStr, name string, f fs.File, srcFS fs.FS) (putResult, error) {
	fi, err := f.Stat()
	if err != nil {
		return putResult{}, err
	}
	if fi.IsDir() {
		return t.createOrUpdateChildDirectory(srcPathStr, name, f, srcFS)
	}
	return t.createOrUpdateChildFile(name, f)
}

func (t *PublicTree) createOrUpdateChildDirectory(srcPathStr, name string, f fs.File, srcFS fs.FS) (putResult, error) {
	dir, ok := f.(fs.ReadDirFile)
	if !ok {
		return putResult{}, fmt.Errorf("cannot read directory contents")
	}
	ents, err := dir.ReadDir(-1)
	if err != nil {
		return putResult{}, fmt.Errorf("reading directory contents: %w", err)
	}

	var tree *PublicTree
	if link := t.userland.Get(name); link != nil {
		tree, err = loadTreeFromCID(t.fs, name, link.Cid)
		if err != nil {
			return putResult{}, err
		}
	} else {
		tree = newEmptyPublicTree(t.fs, name)
	}

	var res putResult
	for _, ent := range ents {
		res, err = tree.Copy(Path{ent.Name()}, filepath.Join(srcPathStr, ent.Name()), srcFS)
		if err != nil {
			return putResult{}, err
		}
	}
	return res, nil
}

func (t *PublicTree) createOrUpdateChildFile(name string, f fs.File) (putResult, error) {
	if link := t.userland.Get(name); link != nil {
		previousFile, err := loadPublicFileFromCID(t.fs, link.Cid, name)
		if err != nil {
			return putResult{}, err
		}

		previousFile.SetContents(f)
		return previousFile.Put()
	}

	ch := newEmptyPublicFile(t.fs, name, f)
	return ch.Put()
}

func (t *PublicTree) updateUserlandLink(name string, res putResult) {
	t.userland.Add(res.ToLink(name))
	t.skeleton[name] = res.ToSkeletonInfo()
	// t.metadata["unixMeta"].(map[string]interface{})["mtime"] = Timestamp().Unix()
}

func (t *PublicTree) removeUserlandLink(name string) {
	t.userland.Remove(name)
	delete(t.skeleton, name)
	// t.metadata["unixMeta"].(map[string]interface{})["mtime"] = Timestamp().Unix()
}

type PublicFile struct {
	fs   merkleDagFS
	name string
	cid  cid.Cid
	size int64

	metadata map[string]interface{}
	previous *cid.Cid
	userland cid.Cid

	content io.ReadCloser
}

var (
	_ mdstore.DagNode = (*PublicFile)(nil)
	_ fs.File         = (*PublicFile)(nil)
)

func newEmptyPublicFile(fs merkleDagFS, name string, content io.ReadCloser) *PublicFile {
	return &PublicFile{
		fs:      fs,
		name:    name,
		content: content,

		metadata: map[string]interface{}{
			"unixMeta": NewUnixMeta(true),
			"isFile":   true,
			"version":  LatestVersion,
		},
	}
}

func loadPublicFileFromCID(fs merkleDagFS, id cid.Cid, name string) (*PublicFile, error) {
	store := fs.DagStore()
	header, err := store.GetNode(id)
	if err != nil {
		return nil, fmt.Errorf("reading file header: %w", err)
	}

	links := header.Links()

	// mdLink := links.Get(metadataLinkName)
	// if mdLink == nil {
	// 	return nil, errors.New("header is missing 'metadata' link")
	// }
	// md, err := loadMetadata(store, mdLink.Cid)
	// if err != nil {
	// 	return nil, err
	// }

	userlandLink := links.Get(userlandLinkName)
	if userlandLink == nil {
		return nil, errors.New("header is missing 'userland' link")
	}

	var previous *cid.Cid
	if prev := links.Get(previousLinkName); prev != nil {
		previous = &prev.Cid
	}

	return &PublicFile{
		fs:   fs,
		cid:  id,
		name: name,
		size: header.Size(),

		// metadata: md,
		previous: previous,
		userland: userlandLink.Cid,
	}, nil
}

func (f *PublicFile) IsFile() bool         { return true }
func (f *PublicFile) Name() string         { return f.name }
func (f *PublicFile) Cid() cid.Cid         { return f.cid }
func (f *PublicFile) Size() int64          { return f.size }
func (f *PublicFile) Links() mdstore.Links { return mdstore.NewLinks() }

func (f *PublicFile) Read(p []byte) (n int, err error) {
	if f.content == nil {
		f.content, err = f.fs.DagStore().GetFile(f.userland)
		if err != nil {
			return 0, err
		}
	}

	return f.content.Read(p)
}

func (f *PublicFile) Close() error {
	if closer, ok := f.content.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (f *PublicFile) Stat() (fs.FileInfo, error) {
	return fsFileInfo{
		name: f.name,
		size: f.size,
		// TODO(b5):
		// mode: f.metadata.UnixMeta.Mode,
		mtime: time.Unix(int64(f.metadata["unixMeta"].(map[string]interface{})["mtime"].(int)), 0),
		sys:   f.fs,
	}, nil
}

func (f *PublicFile) SetContents(r io.ReadCloser) {
	f.content = r
}

func (f *PublicFile) Put() (putResult, error) {
	store := f.fs.DagStore()

	userlandRes, err := store.PutFile(&BareFile{content: f.content})
	if err != nil {
		return putResult{}, fmt.Errorf("putting file %q in store: %w", f.name, err)
	}
	links := mdstore.NewLinks(userlandRes.ToLink(userlandLinkName, true))

	// buf, err := encodeCBOR(f.metadata, nil)
	// if err != nil {
	// 	return putResult{}, fmt.Errorf("encoding file %q metadata: %w", f.name, err)
	// }
	// metadataCid, err := store.PutBlock(buf.Bytes())
	// if err != nil {
	// 	return putResult{}, err
	// }

	// links.Add(mdstore.Link{
	// 	Name:   metadataLinkName,
	// 	Cid:    metadataCid,
	// 	Size:   int64(buf.Len()),
	// 	IsFile: false, // TODO (b5): not sure?
	// })

	if !f.cid.Equals(cid.Cid{}) {
		links.Add(mdstore.Link{
			Name: previousLinkName,
			Cid:  f.cid,
		})
	}

	// write header node
	res, err := store.PutNodeWithData(map[string]interface{}{
		metadataLinkName: f.metadata,
	}, links)
	if err != nil {
		return putResult{}, err
	}

	if !f.cid.Equals(cid.Cid{}) {
		f.previous = &f.cid
	}
	f.cid = res.Cid
	log.Debugw("wrote public file", "name", f.name, "cid", f.cid.String())
	return putResult{
		Cid: res.Cid,
		// TODO(b5)
		// Size: f.Size(),
		// Metadata: metadataCid,
		Userland: userlandRes.Cid,
		IsFile:   true,
	}, nil
}

type PublicHistory interface {
}

type putResult struct {
	Cid      cid.Cid
	Size     int64
	IsFile   bool
	Userland cid.Cid
	Metadata cid.Cid
	Skeleton Skeleton
}

func (r putResult) ToLink(name string) mdstore.Link {
	return mdstore.Link{
		Name:   name,
		Cid:    r.Cid,
		Size:   r.Size,
		IsFile: r.IsFile,
	}
}

func (r putResult) ToSkeletonInfo() SkeletonInfo {
	return SkeletonInfo{
		Cid: r.Cid,
		// Metadata:    r.Metadata,
		Userland:    r.Userland,
		SubSkeleton: r.Skeleton,
		IsFile:      r.IsFile,
	}
}
