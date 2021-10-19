package private

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"sort"
	"time"

	hamt "github.com/filecoin-project/go-hamt-ipld/v3"
	"github.com/fxamacker/cbor/v2"
	cid "github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	golog "github.com/ipfs/go-log"
	"github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/mdstore"
	"github.com/qri-io/wnfs-go/public"
)

var log = golog.Logger("wnfs")

// FS is a *private* filestystem, with methods for externalizing private data
type FS interface {
	RootKey() Key
	PrivateName() (Name, error)
}

type Root struct {
	*Tree
	ctx         context.Context
	store       mdstore.PrivateStore
	hamt        *hamt.Node
	hamtRootCID *cid.Cid
}

var (
	_ base.PrivateMerkleDagFS = (*Root)(nil)
	_ mdstore.DagNode         = (*Root)(nil)
	_ base.Tree               = (*Root)(nil)
	_ fs.File                 = (*Root)(nil)
	_ fs.ReadDirFile          = (*Root)(nil)
)

func NewEmptyRoot(ctx context.Context, store mdstore.PrivateStore, name string, rootKey Key) (*Root, error) {
	hamtRoot, err := hamt.NewNode(ipldcbor.NewCborStore(store.Blockservice().Blockstore()))
	if err != nil {
		return nil, err
	}

	root := &Root{
		ctx:   ctx,
		store: store,
		hamt:  hamtRoot,
	}

	private, err := NewEmptyTree(root, IdentityBareNamefilter(), name)
	if err != nil {
		return nil, err
	}
	root.Tree = private
	return root, nil
}

func LoadRoot(ctx context.Context, store mdstore.PrivateStore, name string, hamtCID cid.Cid, rootKey Key, rootName Name) (*Root, error) {
	var (
		hamtRoot      *hamt.Node
		privateTree   *Tree
		ipldCBORStore = ipldcbor.NewCborStore(store.Blockservice().Blockstore())
	)
	if rootName == Name("") {
		return nil, fmt.Errorf("privateName is required")
	}

	log.Debugw("loading HAMT", "cid", hamtCID)
	hamtRoot, err := hamt.LoadNode(ctx, ipldCBORStore, hamtCID)
	if err != nil {
		return nil, fmt.Errorf("opening private root:\n%w", err)
	}

	data := CborByteArray{}
	exists, err := hamtRoot.Find(ctx, string(rootName), &data)
	if err != nil {
		return nil, fmt.Errorf("opening private root: %w", err)
	} else if !exists {
		return nil, fmt.Errorf("opening private root: %w", base.ErrNotFound)
	}
	_, privateRoot, err := cid.CidFromBytes([]byte(data))
	if err != nil {
		return nil, fmt.Errorf("reading CID bytes: %w", err)
	}

	root := &Root{
		ctx:         ctx,
		store:       store,
		Tree:        privateTree,
		hamt:        hamtRoot,
		hamtRootCID: &hamtCID,
	}

	// if privateRoot, err := mmpt.Get(string(rootName)); err == nil {
	privateTree, err = LoadTree(root, name, rootKey, privateRoot)
	if err != nil {
		return nil, err
	}
	root.Tree = privateTree
	return root, nil
}

func (r *Root) Context() context.Context           { return r.ctx }
func (r *Root) Cid() cid.Cid                       { return *r.hamtRootCID }
func (r *Root) HAMT() *hamt.Node                   { return r.hamt }
func (r *Root) PrivateStore() mdstore.PrivateStore { return r.store }

func (r *Root) Open(pathStr string) (fs.File, error) {
	path, err := base.NewPath(pathStr)
	if err != nil {
		return nil, err
	}

	return r.Get(path)
}

func (r *Root) Add(path base.Path, f fs.File) (res base.PutResult, err error) {
	res, err = r.Tree.Add(path, f)
	if err != nil {
		return nil, err
	}
	return res, r.putHamt()
}

func (r *Root) Copy(path base.Path, srcPathStr string, srcFS fs.FS) (res base.PutResult, err error) {
	res, err = r.Tree.Copy(path, srcPathStr, srcFS)
	if err != nil {
		return nil, err
	}
	return res, r.putHamt()
}

func (r *Root) Rm(path base.Path) (base.PutResult, error) {
	res, err := r.Tree.Rm(path)
	if err != nil {
		return nil, err
	}
	return res, r.putHamt()
}

func (r *Root) Mkdir(path base.Path) (res base.PutResult, err error) {
	res, err = r.Tree.Mkdir(path)
	if err != nil {
		return nil, err
	}
	return res, r.putHamt()
}

func (r *Root) Put() (base.PutResult, error) {
	res, err := r.Tree.Put()
	if err != nil {
		return nil, err
	}
	err = r.putHamt()
	return res, err
}

func (r *Root) putHamt() error {
	if r.hamt != nil {
		id, err := r.hamt.Write(r.ctx)
		if err != nil {
			return err
		}
		log.Debugw("writing HAMT", "cid", id)
		r.hamtRootCID = &id
	}
	return nil
}

func (r *Root) MergeDiverged(b base.Node) (result base.MergeResult, err error) {
	return result, fmt.Errorf("unfinished: private.Root.MergeDiverged")
}

type TreeInfo struct {
	INum     INumber
	Size     int64
	Bnf      BareNamefilter
	Ratchet  string
	Links    PrivateLinks
	Metadata *base.Metadata
}

type Tree struct {
	fs      base.PrivateMerkleDagFS
	ratchet *SpiralRatchet
	name    string  // not stored on the node. used to satisfy fs.File interface
	cid     cid.Cid // header node cid this tree was loaded from. empty if unstored
	// header
	info TreeInfo

	// TODO(b5): private history
	// history History
}

var (
	_ mdstore.DagNode = (*Tree)(nil)
	_ base.Tree       = (*Tree)(nil)
	_ fs.File         = (*Tree)(nil)
	_ fs.ReadDirFile  = (*Tree)(nil)
)

func NewEmptyTree(fs base.PrivateMerkleDagFS, parent BareNamefilter, name string) (*Tree, error) {
	in := NewINumber()
	bnf, err := NewBareNamefilter(parent, in)
	if err != nil {
		return nil, err
	}

	return &Tree{
		fs:      fs,
		ratchet: NewSpiralRatchet(),
		name:    name,
		info: TreeInfo{
			INum:  in,
			Bnf:   bnf,
			Links: PrivateLinks{},
			Metadata: &base.Metadata{
				UnixMeta: base.NewUnixMeta(false),
				IsFile:   false,
				Version:  base.LatestVersion,
			},
		},
	}, nil
}

func LoadTree(fs base.PrivateMerkleDagFS, name string, key Key, id cid.Cid) (*Tree, error) {
	log.Debugw("LoadTree", "name", name, "cid", id)

	f, err := fs.PrivateStore().GetEncryptedFile(id, key[:])
	if err != nil {
		log.Debugw("LoadTree", "err", err)
		return nil, err
	}
	defer f.Close()

	info := TreeInfo{}
	if err := cbor.NewDecoder(f).Decode(&info); err != nil {
		log.Debugw("LoadTree", "err", err)
		return nil, err
	}

	ratchet, err := DecodeRatchet(info.Ratchet)
	if err != nil {
		return nil, fmt.Errorf("decoding ratchet: %w", err)
	}
	info.Ratchet = ""

	return &Tree{
		fs:      fs,
		name:    name,
		ratchet: ratchet,
		cid:     id,
		info:    info,
	}, nil
}

func LoadTreeFromName(fs base.PrivateMerkleDagFS, key Key, name string, pn Name) (*Tree, error) {
	exists, data, err := fs.HAMT().FindRaw(fs.Context(), string(pn))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, base.ErrNotFound
	}

	_, id, err := cid.CidFromBytes(data)
	if err != nil {
		return nil, err
	}

	return LoadTree(fs, name, key, id)
}

func (pt *Tree) BareNamefilter() BareNamefilter { return pt.info.Bnf }
func (pt *Tree) INumber() INumber               { return pt.info.INum }
func (pt *Tree) Cid() cid.Cid                   { return pt.cid }
func (pt *Tree) Links() mdstore.Links           { return mdstore.NewLinks() } // TODO(b5): private links
func (pt *Tree) Size() int64                    { return pt.info.Size }
func (pt *Tree) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		// TODO(b5): finish
	}
}
func (pt *Tree) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   pt.name,
		Cid:    pt.cid,
		Size:   pt.info.Size,
		IsFile: false,
		Mtime:  pt.info.Metadata.UnixMeta.Mtime,
	}
}

func (pt *Tree) PrivateName() (Name, error) {
	knf, err := AddKey(pt.info.Bnf, pt.ratchet.Key())
	if err != nil {
		return "", err
	}
	return ToName(knf)
}
func (pt *Tree) Key() Key { return pt.ratchet.Key() }

func (pt *Tree) Stat() (fs.FileInfo, error) {
	return base.NewFSFileInfo(
		pt.name,
		pt.info.Size,
		// TODO (b5):
		// mode:  t.metadata.UnixMeta.Mode,
		fs.ModeDir,
		time.Unix(pt.info.Metadata.UnixMeta.Mtime, 0),
		pt.fs,
	), nil
}

func (pt *Tree) Read(p []byte) (n int, err error) {
	return -1, fmt.Errorf("cannot read directory")
}
func (pt *Tree) Close() error { return nil }

func (pt *Tree) ReadDir(n int) ([]fs.DirEntry, error) {
	if n < 0 {
		n = len(pt.info.Links)
	}

	entries := make([]fs.DirEntry, 0, n)
	for i, link := range pt.info.Links.SortedSlice() {
		entries = append(entries, base.NewFSDirEntry(link.Name, link.IsFile))

		if i == n {
			break
		}
	}
	return entries, nil
}

func (pt *Tree) Add(path base.Path, f fs.File) (res base.PutResult, err error) {
	log.Debugw("Tree.Add", "path", path)
	if len(path) == 0 {
		return res, errors.New("invalid path: empty")
	}

	head, tail := path.Shift()
	if tail == nil {
		res, err = pt.createOrUpdateChildFile(head, f)
		if err != nil {
			return res, err
		}
	} else {
		childDir, err := pt.getOrCreateDirectChildTree(head)
		if err != nil {
			return res, err
		}

		// recurse
		res, err = childDir.Add(tail, f)
		if err != nil {
			return res, err
		}
	}

	pt.updateUserlandLink(head, res)
	// contents of tree have changed, write an update.
	return pt.Put()
}

func (pt *Tree) Copy(path base.Path, srcPathStr string, srcFS fs.FS) (res base.PutResult, err error) {
	log.Debugw("Tree.copy", "path", path, "srcPath", srcPathStr)
	if len(path) == 0 {
		return res, errors.New("invalid path: empty")
	}

	head, tail := path.Shift()
	if tail == nil {
		f, err := srcFS.Open(srcPathStr)
		if err != nil {
			return nil, err
		}

		res, err = pt.createOrUpdateChild(srcPathStr, head, f, srcFS)
		if err != nil {
			return res, err
		}
	} else {
		childDir, err := pt.getOrCreateDirectChildTree(head)
		if err != nil {
			return res, err
		}

		// recurse
		res, err = childDir.Copy(tail, srcPathStr, srcFS)
		if err != nil {
			return res, err
		}
	}

	pt.updateUserlandLink(head, res)
	// contents of tree have changed, write an update.
	return pt.Put()
}

func (pt *Tree) Get(path base.Path) (fs.File, error) {
	head, tail := path.Shift()
	if head == "" {
		return pt, nil
	}

	link := pt.info.Links.Get(head)
	if link == nil {
		return nil, base.ErrNotFound
	}

	if tail != nil {
		ch, err := LoadTree(pt.fs, head, link.Key, link.Cid)
		if err != nil {
			return nil, err
		}

		// recurse
		return ch.Get(tail)
	}

	if link.IsFile {
		return LoadFileFromCID(pt.fs, head, link.Key, link.Cid)
	}

	return LoadTree(pt.fs, link.Name, link.Key, link.Cid)
}

func (pt *Tree) Rm(path base.Path) (base.PutResult, error) {
	head, tail := path.Shift()
	if head == "" {
		return nil, fmt.Errorf("invalid path: empty")
	}

	if tail == nil {
		pt.removeUserlandLink(head)
	} else {
		link := pt.info.Links.Get(head)
		if link == nil {
			return nil, base.ErrNotFound
		}
		child, err := LoadTree(pt.fs, link.Name, link.Key, link.Cid)
		if err != nil {
			return nil, err
		}

		// recurse
		res, err := child.Rm(tail)
		if err != nil {
			return nil, err
		}
		pt.updateUserlandLink(head, res)
	}

	// contents of tree have changed, write an update.
	return pt.Put()
}

func (pt *Tree) Mkdir(path base.Path) (res base.PutResult, err error) {
	if len(path) < 1 {
		return res, errors.New("invalid path: empty")
	}

	head, tail := path.Shift()
	childDir, err := pt.getOrCreateDirectChildTree(head)
	if err != nil {
		return nil, err
	}

	if tail == nil {
		res, err = childDir.Put()
		if err != nil {
			return nil, err
		}
	} else {
		res, err = pt.Mkdir(tail)
		if err != nil {
			return nil, err
		}
	}

	pt.updateUserlandLink(head, res)
	return pt.Put()
}

func (pt *Tree) getOrCreateDirectChildTree(name string) (*Tree, error) {
	link := pt.info.Links.Get(name)
	if link == nil {
		return NewEmptyTree(pt.fs, pt.info.Bnf, name)
	}

	return LoadTree(pt.fs, name, link.Key, link.Cid)
}

func (pt *Tree) createOrUpdateChild(srcPathStr, name string, f fs.File, srcFS fs.FS) (base.PutResult, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return pt.createOrUpdateChildDirectory(srcPathStr, name, f, srcFS)
	}
	return pt.createOrUpdateChildFile(name, f)
}

func (pt *Tree) createOrUpdateChildDirectory(srcPathStr, name string, f fs.File, srcFS fs.FS) (base.PutResult, error) {
	dir, ok := f.(fs.ReadDirFile)
	if !ok {
		return nil, fmt.Errorf("cannot read directory contents")
	}
	ents, err := dir.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("reading directory contents: %w", err)
	}

	var tree *Tree
	if link := pt.info.Links.Get(name); link != nil {
		tree, err = LoadTree(pt.fs, link.Name, link.Key, link.Cid)
		if err != nil {
			return nil, err
		}
	} else {
		tree, err = NewEmptyTree(pt.fs, pt.info.Bnf, name)
		if err != nil {
			return nil, err
		}
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

func (pt *Tree) createOrUpdateChildFile(name string, f fs.File) (base.PutResult, error) {
	if link := pt.info.Links.Get(name); link != nil {
		previousFile, err := LoadFileFromCID(pt.fs, link.Name, link.Key, link.Cid)
		if err != nil {
			return nil, err
		}
		previousFile.SetContents(f)
		return previousFile.Put()
	}

	ch, err := NewFile(pt.fs, pt.info.Bnf, f)
	if err != nil {
		return nil, err
	}
	return ch.Put()
}

func (pt *Tree) Put() (base.PutResult, error) {
	log.Debugw("Tree.Put", "name", pt.name)

	pt.ratchet.Inc()
	key := pt.ratchet.Key()
	pt.info.Ratchet = pt.ratchet.Encode()
	pt.info.Size = pt.info.Links.SizeSum()

	buf := &bytes.Buffer{}
	if err := cbor.NewEncoder(buf).Encode(pt.info); err != nil {
		return nil, err
	}

	res, err := pt.fs.PrivateStore().PutEncryptedFile(base.NewMemfileReader("", buf), key[:])
	if err != nil {
		return nil, err
	}

	privName, err := pt.PrivateName()
	if err != nil {
		return nil, err
	}

	idBytes := CborByteArray(res.Cid.Bytes())
	if err := pt.fs.HAMT().Set(pt.fs.Context(), string(privName), &idBytes); err != nil {
		return nil, err
	}

	pt.cid = res.Cid
	log.Debugw("Tree.Put", "name", pt.name, "cid", pt.cid.String(), "size", pt.info.Size)
	return PutResult{
		PutResult: public.PutResult{
			Cid:  pt.cid,
			Size: pt.info.Size,
		},
		Key:     key,
		Pointer: privName,
	}, nil
}

func (pt *Tree) updateUserlandLink(name string, res base.PutResult) {
	pt.info.Links.Add(res.(PutResult).ToPrivateLink(name))
	pt.info.Metadata.UnixMeta.Mtime = base.Timestamp().Unix()
}

func (pt *Tree) removeUserlandLink(name string) {
	pt.info.Links.Remove(name)
	pt.info.Metadata.UnixMeta.Mtime = base.Timestamp().Unix()
}

func (pt *Tree) MergeDiverged(n base.Node) (result base.MergeResult, err error) {
	return result, fmt.Errorf("unfinished: private.Tree.MergeDiverged")
}

type File struct {
	fs      base.PrivateMerkleDagFS
	ratchet *SpiralRatchet
	name    string  // not persisted. used to implement fs.File interface
	cid     cid.Cid // cid header was loaded from. empty if new
	info    FileInfo
	content io.ReadCloser
}

type FileInfo struct {
	INumber        INumber
	Size           int64
	BareNamefilter BareNamefilter
	Ratchet        string
	Metadata       *base.Metadata
	ContentID      cid.Cid
}

var _ fs.File = (*File)(nil)

func NewFile(fs base.PrivateMerkleDagFS, parent BareNamefilter, f fs.File) (*File, error) {
	in := NewINumber()
	bnf, err := NewBareNamefilter(parent, in)
	if err != nil {
		return nil, err
	}

	return &File{
		fs:      fs,
		ratchet: NewSpiralRatchet(),
		content: f,
		info: FileInfo{
			INumber:        in,
			BareNamefilter: bnf,
			Metadata: &base.Metadata{
				UnixMeta: base.NewUnixMeta(true),
				IsFile:   true,
				Version:  base.LatestVersion,
			},
		},
	}, nil
}

func LoadFileFromCID(fs base.PrivateMerkleDagFS, name string, key Key, id cid.Cid) (*File, error) {
	store := fs.PrivateStore()
	f, err := store.GetEncryptedFile(id, key[:])
	if err != nil {
		log.Debugw("LoadFileFromCID", "err", err)
		return nil, err
	}

	info := FileInfo{}
	if err := cbor.NewDecoder(f).Decode(&info); err != nil {
		return nil, err
	}

	ratchet, err := DecodeRatchet(info.Ratchet)
	if err != nil {
		return nil, err
	}
	info.Ratchet = ""

	// TODO(b5): lazy-load on first call to Read()
	content, err := store.GetEncryptedFile(info.ContentID, key[:])
	if err != nil {
		return nil, err
	}

	return &File{
		fs:      fs,
		ratchet: ratchet,
		name:    name,
		cid:     id,
		info:    info,
		content: content,
	}, nil
}

func (pf *File) BareNamefilter() BareNamefilter { return pf.info.BareNamefilter }
func (pf *File) INumber() INumber               { return pf.info.INumber }
func (pf *File) Cid() cid.Cid                   { return pf.cid }
func (pf *File) Content() cid.Cid               { return pf.info.ContentID }

func (pf *File) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		// TODO(b5): finish
	}
}

func (pf *File) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   pf.name,
		Cid:    pf.cid,
		Size:   pf.info.Size,
		IsFile: true,
		Mtime:  pf.info.Metadata.UnixMeta.Mtime,
	}
}

func (pf *File) Name() (Name, error) {
	knf, err := AddKey(pf.info.BareNamefilter, pf.ratchet.Key())
	if err != nil {
		return "", err
	}
	return ToName(knf)
}
func (pf *File) Key() Key { return pf.ratchet.Key() }

func (pf *File) Stat() (fs.FileInfo, error) {
	return base.NewFSFileInfo(
		pf.name,
		pf.info.Size,
		// TODO(b5):
		// mode:  pf.info.Metadata.UnixMeta.Mode,
		fs.FileMode(pf.info.Metadata.UnixMeta.Mode),
		time.Unix(pf.info.Metadata.UnixMeta.Mtime, 0),
		pf.fs,
	), nil
}

func (pf *File) Read(p []byte) (n int, err error) { return pf.content.Read(p) }
func (pf *File) Close() error                     { return pf.content.Close() }

func (pf *File) SetContents(f fs.File) {
	pf.content = f
}

func (pf *File) Put() (PutResult, error) {
	store := pf.fs.PrivateStore()

	// generate a new version key by advancing the ratchet
	// TODO(b5): what happens if anything errors after advancing the ratchet?
	// assuming we need to make a point of throwing away the file & cleaning the MMPT
	pf.ratchet.Inc()
	key := pf.ratchet.Key()

	res, err := store.PutEncryptedFile(base.NewMemfileReader(pf.name, pf.content), key[:])
	if err != nil {
		return PutResult{}, err
	}

	// update header details
	pf.info.ContentID = res.Cid
	pf.info.Size = res.Size
	pf.info.Ratchet = pf.ratchet.Encode()
	pf.info.Metadata.UnixMeta.Mtime = base.Timestamp().Unix()

	buf := &bytes.Buffer{}
	if err := cbor.NewEncoder(buf).Encode(pf.info); err != nil {
		return PutResult{}, err
	}
	headerRes, err := store.PutEncryptedFile(base.NewMemfileReader(pf.name, buf), key[:])
	if err != nil {
		return PutResult{}, err
	}

	// create private name from key
	privName, err := pf.Name()
	if err != nil {
		return PutResult{}, err
	}

	idBytes := CborByteArray(headerRes.Cid.Bytes())
	if err := pf.fs.HAMT().Set(pf.fs.Context(), string(privName), &idBytes); err != nil {
		return PutResult{}, err
	}

	pf.cid = headerRes.Cid
	log.Debugw("File.Put", "name", pf.name, "cid", pf.cid.String(), "size", res.Size)
	return PutResult{
		PutResult: public.PutResult{
			Cid:      headerRes.Cid,
			IsFile:   true,
			Userland: res.Cid,
			Size:     res.Size,
		},
		Key:     key,
		Pointer: privName,
	}, nil
}

type INumber [32]byte

func NewINumber() INumber {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	id := [32]byte{}
	for i, v := range buf {
		id[i] = v
	}
	return id
}

type PrivateLink struct {
	mdstore.Link
	Key     Key
	Pointer Name
}

type PrivateLinks map[string]PrivateLink

func (pls PrivateLinks) Get(name string) *PrivateLink {
	l, ok := pls[name]
	if !ok {
		return nil
	}
	return &l
}

func (pls PrivateLinks) Add(link PrivateLink) {
	pls[link.Name] = link
}

func (pls PrivateLinks) Remove(name string) bool {
	_, existed := pls[name]
	delete(pls, name)
	return existed
}

func (pls PrivateLinks) SortedSlice() []PrivateLink {
	names := make([]string, 0, len(pls))
	for name := range pls {
		names = append(names, name)
	}
	sort.Strings(names)

	links := make([]PrivateLink, 0, len(pls))
	for _, name := range names {
		links = append(links, pls[name])
	}
	return links
}

func (pls PrivateLinks) SizeSum() (total int64) {
	for _, l := range pls {
		total += l.Size
	}
	return total
}

type PutResult struct {
	public.PutResult

	Key     Key
	Pointer Name
}

func (r PutResult) ToPrivateLink(name string) PrivateLink {
	return PrivateLink{
		Link:    r.ToLink(name),
		Key:     r.Key,
		Pointer: r.Pointer,
	}
}

func Merge(a, b *Root) (result base.MergeResult, err error) {
	// 1. Merge root CIDs
	// 2. Merge HAMTS
	return base.MergeResult{}, fmt.Errorf("unfinished: private Merge")
}
