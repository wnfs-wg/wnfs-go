package wnfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

type PublicTree struct {
	store mdstore.MerkleDagStore // embed a reference to store this tree is associated with
	name  string                 // directory name, used while linking
	cid   cid.Cid
	size  int64

	// header data
	metadata *Metadata
	skeleton Skeleton
	previous *cid.Cid
	userland mdstore.Links // links to files are stored in "userland" header key
}

func newEmptyPublicTree(store mdstore.MerkleDagStore, name string) *PublicTree {
	now := Timestamp().Unix()

	return &PublicTree{
		store: store,
		name:  name,

		userland: mdstore.NewLinks(),
		metadata: &Metadata{
			UnixMeta: &UnixMeta{
				Ctime: now,
				Mtime: now,
			},
			Version: LatestVersion,
		},
		skeleton: Skeleton{},
	}
}

func loadTreeFromCID(store mdstore.MerkleDagStore, name string, id cid.Cid) (*PublicTree, error) {
	log.Debugw("loadTreeFromCID", "name", name, "cid", id)
	header, err := store.GetNode(id)
	if err != nil {
		return nil, err
	}

	links := header.Links()

	mdLnk := links.Get(metadataLinkName)
	if mdLnk == nil {
		return nil, errors.New("header is missing 'metadata' link")
	}
	md, err := loadMetadata(store, mdLnk.Cid)
	if err != nil {
		return nil, err
	}
	if md.IsFile {
		return nil, errors.New("expected file to be a tree")
	}

	skLnk := links.Get(skeletonLinkName)
	if skLnk == nil {
		return nil, errors.New("header is missing 'skeleton' link")
	}
	sk, err := loadSkeleton(store, skLnk.Cid)
	if err != nil {
		return nil, err
	}

	var previous *cid.Cid
	if prev := links.Get(previousLinkName); prev != nil {
		previous = &prev.Cid
	}

	userlandLnk := links.Get(userlandLinkName)
	if userlandLnk == nil {
		return nil, errors.New("header is missing 'userland' link")
	}
	userland, err := store.GetNode(userlandLnk.Cid)
	if err != nil {
		return nil, err
	}

	return &PublicTree{
		store: store,
		name:  name,

		cid:      header.Cid(),
		metadata: md,
		skeleton: sk,
		previous: previous,
		userland: userland.Links(),
	}, nil
}

var (
	_ mdstore.DagNode = (*PublicTree)(nil)
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
		name:  t.name,
		size:  t.size,
		mode:  t.metadata.UnixMeta.Mode,
		mtime: time.Unix(t.metadata.UnixMeta.Mtime, 0),
		sys:   t.store,
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
		metadata: *t.metadata,
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
		ch, err := loadTreeFromCID(t.store, head, link.Cid)
		if err != nil {
			return nil, err
		}

		// recurse
		return ch.Get(tail)
	}

	if t.skeleton[head].IsFile {
		return loadPublicFileFromCID(t.store, link.Cid, link.Name)
	}

	return loadTreeFromCID(t.store, link.Name, link.Cid)
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

	t.updateLink(head, res)
	// contents of tree have changed, write an update.
	// TODO(b5) - pretty sure this is a bug if multiple writes are batched in the
	// same "publish" / transaction. Write advances the previous / current CID,
	// so if the same directory is mutated multiple times before the next snapshot
	// we'll have intermediate states as the "previous" pointer
	return t.Put()
}

func (t *PublicTree) Put() (putResult, error) {
	userlandResult, err := t.store.PutNode(t.userland)
	if err != nil {
		return putResult{}, err
	}

	links := mdstore.NewLinks(userlandResult.ToLink(userlandLinkName, false))

	for _, filer := range []CBORFiler{
		t.metadata,
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

		res, err := t.store.PutFile(file)
		if err != nil {
			return putResult{}, err
		}

		links.Add(res.ToLink(linkName, true))
	}

	result, err := t.writeHeader(links)
	if err != nil {
		return putResult{}, err
	}

	t.cid = result.Cid
	log.Debugw("wrote public tree", "name", t.name, "cid", t.cid.String(), "links", links.Map())
	return result, nil
}

func (t *PublicTree) writeHeader(links mdstore.Links) (putResult, error) {
	if !t.cid.Equals(cid.Cid{}) {
		// TODO(b5): incorrect
		n, err := t.store.GetNode(t.cid)
		if err != nil {
			return putResult{}, err
		}

		links.Add(mdstore.Link{
			Name: previousLinkName,
			Cid:  t.cid,
			Size: n.Size(),
		})
	}

	headerNode, err := t.store.PutNode(links)
	if err != nil {
		return putResult{}, err
	}

	return putResult{
		Cid:      headerNode.Cid,
		Size:     headerNode.Size,
		Metadata: links.Get(metadataLinkName).Cid,
		Userland: links.Get(userlandLinkName).Cid,
		Skeleton: t.skeleton,
	}, nil
}

func (t *PublicTree) getOrCreateDirectChildTree(name string) (*PublicTree, error) {
	link := t.userland.Get(name)
	if link == nil {
		return newEmptyPublicTree(t.store, name), nil
	}

	return loadTreeFromCID(t.store, name, link.Cid)
}

func (t *PublicTree) createOrUpdateChildFile(name string, f fs.File) (putResult, error) {
	if link := t.userland.Get(name); link != nil {
		previousFile, err := loadPublicFileFromCID(t.store, link.Cid, name)
		if err != nil {
			return putResult{}, err
		}

		previousFile.SetContents(f)
		return previousFile.Put()
	}

	ch := newEmptyPublicFile(t.store, name, f)
	return ch.Put()
}

func (t *PublicTree) updateLink(name string, res putResult) {
	t.userland.Add(res.ToLink(name))
	t.skeleton[name] = res.ToSkeletonInfo()
	t.metadata.UnixMeta.Mtime = Timestamp().Unix()
}

type PublicFile struct {
	store mdstore.MerkleDagStore
	name  string
	cid   cid.Cid
	size  int64

	metadata *Metadata
	previous *cid.Cid
	userland cid.Cid

	content io.ReadCloser
}

var (
	_ mdstore.DagNode = (*PublicFile)(nil)
	_ fs.File         = (*PublicFile)(nil)
)

func newEmptyPublicFile(store mdstore.MerkleDagStore, name string, content io.ReadCloser) *PublicFile {
	now := Timestamp().Unix()
	return &PublicFile{
		metadata: &Metadata{
			UnixMeta: &UnixMeta{
				Ctime: now,
				Mtime: now,
			},
			IsFile:  true,
			Version: LatestVersion,
		},
		store:   store,
		name:    name,
		content: content,
	}
}

func loadPublicFileFromCID(store mdstore.MerkleDagStore, id cid.Cid, name string) (*PublicFile, error) {
	header, err := store.GetNode(id)
	if err != nil {
		return nil, fmt.Errorf("reading file header: %w", err)
	}

	links := header.Links()

	mdLink := links.Get(metadataLinkName)
	if mdLink == nil {
		return nil, errors.New("header is missing 'metadata' link")
	}
	md, err := loadMetadata(store, mdLink.Cid)
	if err != nil {
		return nil, err
	}

	userlandLink := links.Get(userlandLinkName)
	if userlandLink == nil {
		return nil, errors.New("header is missing 'userland' link")
	}

	var previous *cid.Cid
	if prev := links.Get(previousLinkName); prev != nil {
		previous = &prev.Cid
	}

	return &PublicFile{
		store: store,
		cid:   id,
		name:  name,

		metadata: md,
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
		f.content, err = f.store.GetFile(f.userland)
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
		// mtime: time.Unix(f.metadata.UnixMeta.Mtime, 0),
		sys: f.store,
	}, nil
}

func (f *PublicFile) SetContents(r io.ReadCloser) {
	f.content = r
}

func (f *PublicFile) Put() (putResult, error) {
	buf, err := encodeCBOR(f.metadata, nil)
	if err != nil {
		return putResult{}, fmt.Errorf("encoding file %q metadata: %w", f.name, err)
	}
	metadataCid, err := f.store.PutBlock(buf.Bytes())
	if err != nil {
		return putResult{}, err
	}
	links := mdstore.NewLinks(mdstore.Link{
		Name:   metadataLinkName,
		Cid:    metadataCid,
		Size:   int64(buf.Len()),
		IsFile: false, // TODO (b5): not sure?
	})

	userlandRes, err := f.store.PutFile(&BareFile{content: f.content})
	if err != nil {
		return putResult{}, fmt.Errorf("putting file %q in store: %w", f.name, err)
	}
	links.Add(userlandRes.ToLink(userlandLinkName, true))

	if !f.cid.Equals(cid.Cid{}) {
		links.Add(mdstore.Link{
			Name: previousLinkName,
			Cid:  f.cid,
		})
	}

	// write header node
	res, err := f.store.PutNode(links)
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
		Metadata: metadataCid,
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
		Cid:         r.Cid,
		Metadata:    r.Metadata,
		Userland:    r.Userland,
		SubSkeleton: r.Skeleton,
		IsFile:      r.IsFile,
	}
}
