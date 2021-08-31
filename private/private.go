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

	"github.com/fxamacker/cbor/v2"
	cid "github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log"
	"github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/mdstore"
	"github.com/qri-io/wnfs-go/public"
)

var log = golog.Logger("wnfs/public")

type PrivateFS interface {
	RootKey() Key
	PrivateName() (PrivateName, error)
}

type DecryptedNode interface {
	base.Node
	INumber() INumber
	BareNamefilter() BareNamefilter
	PrivateName() (PrivateName, error)
	Key() Key
}

type DecryptedFile interface {
	DecryptedNode
	Content() cid.Cid
}

type DecryptedTree interface {
	DecryptedNode
}

type PrivateTreeInfo struct {
	INum     INumber
	Size     int64
	Bnf      BareNamefilter
	Ratchet  string
	Links    PrivateLinks
	Metadata *base.Metadata
}

type PrivateTree struct {
	fs      base.MerkleDagFS
	ratchet *SpiralRatchet
	name    string  // not stored on the node. used to satisfy fs.File interface
	cid     cid.Cid // header node cid this tree was loaded from. empty if unstored
	// header
	info PrivateTreeInfo

	// TODO(b5): private history
	// history History
}

var (
	_ mdstore.DagNode = (*PrivateTree)(nil)
	_ DecryptedTree   = (*PrivateTree)(nil)
	_ base.Tree       = (*PrivateTree)(nil)
	_ fs.File         = (*PrivateTree)(nil)
	_ fs.ReadDirFile  = (*PrivateTree)(nil)
)

func NewEmptyTree(fs base.MerkleDagFS, parent BareNamefilter, name string) (*PrivateTree, error) {
	in := NewINumber()
	bnf, err := NewBareNamefilter(parent, in)
	if err != nil {
		return nil, err
	}

	return &PrivateTree{
		fs:      fs,
		ratchet: NewSpiralRatchet(),
		name:    name,
		info: PrivateTreeInfo{
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

func LoadTree(fs base.MerkleDagFS, name string, key Key, id cid.Cid) (*PrivateTree, error) {
	log.Debugw("LoadTree", "name", name, "cid", id)

	f, err := fs.DagStore().GetEncryptedFile(id, key[:])
	if err != nil {
		log.Debugw("LoadTree", "err", err)
		return nil, err
	}
	defer f.Close()

	info := PrivateTreeInfo{}
	if err := cbor.NewDecoder(f).Decode(&info); err != nil {
		log.Debugw("LoadTree", "err", err)
		return nil, err
	}

	ratchet, err := DecodeRatchet(info.Ratchet)
	if err != nil {
		return nil, fmt.Errorf("decoding ratchet: %w", err)
	}
	info.Ratchet = ""

	return &PrivateTree{
		fs:      fs,
		name:    name,
		ratchet: ratchet,
		cid:     id,
		info:    info,
	}, nil
}

func LoadPrivateTreeFromPrivateName(fs base.MerkleDagFS, key Key, name string, pn PrivateName) (*PrivateTree, error) {
	exists, data, err := fs.HAMT().FindRaw(context.TODO(), string(pn))
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

func (pt *PrivateTree) BareNamefilter() BareNamefilter { return pt.info.Bnf }
func (pt *PrivateTree) INumber() INumber               { return pt.info.INum }
func (pt *PrivateTree) Cid() cid.Cid                   { return pt.cid }
func (pt *PrivateTree) Links() mdstore.Links           { return mdstore.NewLinks() } // TODO(b5): private links
func (pt *PrivateTree) Size() int64                    { return pt.info.Size }
func (pt *PrivateTree) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		// TODO(b5): finish
	}
}

func (pt *PrivateTree) PrivateName() (PrivateName, error) {
	knf, err := AddKey(pt.info.Bnf, pt.ratchet.Key())
	if err != nil {
		return "", err
	}
	return ToPrivateName(knf)
}
func (pt *PrivateTree) Key() Key { return pt.ratchet.Key() }

func (pt *PrivateTree) Stat() (fs.FileInfo, error) {
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

func (pt *PrivateTree) Read(p []byte) (n int, err error) {
	return -1, fmt.Errorf("cannot read directory")
}
func (pt *PrivateTree) Close() error { return nil }

func (pt *PrivateTree) ReadDir(n int) ([]fs.DirEntry, error) {
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

func (pt *PrivateTree) Add(path base.Path, f fs.File) (res base.PutResult, err error) {
	log.Debugw("PrivateTree.Add", "path", path)
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

func (pt *PrivateTree) Copy(path base.Path, srcPathStr string, srcFS fs.FS) (res base.PutResult, err error) {
	log.Debugw("PrivateTree.copy", "path", path, "srcPath", srcPathStr)
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

func (pt *PrivateTree) Get(path base.Path) (fs.File, error) {
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
		return LoadPrivateFileFromCID(pt.fs, head, link.Key, link.Cid)
	}

	return LoadTree(pt.fs, link.Name, link.Key, link.Cid)
}

func (pt *PrivateTree) Rm(path base.Path) (base.PutResult, error) {
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

func (pt *PrivateTree) Mkdir(path base.Path) (res base.PutResult, err error) {
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

func (pt *PrivateTree) getOrCreateDirectChildTree(name string) (*PrivateTree, error) {
	link := pt.info.Links.Get(name)
	if link == nil {
		return NewEmptyTree(pt.fs, pt.info.Bnf, name)
	}

	return LoadTree(pt.fs, name, link.Key, link.Cid)
}

func (pt *PrivateTree) createOrUpdateChild(srcPathStr, name string, f fs.File, srcFS fs.FS) (base.PutResult, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return pt.createOrUpdateChildDirectory(srcPathStr, name, f, srcFS)
	}
	return pt.createOrUpdateChildFile(name, f)
}

func (pt *PrivateTree) createOrUpdateChildDirectory(srcPathStr, name string, f fs.File, srcFS fs.FS) (base.PutResult, error) {
	dir, ok := f.(fs.ReadDirFile)
	if !ok {
		return nil, fmt.Errorf("cannot read directory contents")
	}
	ents, err := dir.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("reading directory contents: %w", err)
	}

	var tree *PrivateTree
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

func (pt *PrivateTree) createOrUpdateChildFile(name string, f fs.File) (base.PutResult, error) {
	if link := pt.info.Links.Get(name); link != nil {
		previousFile, err := LoadPrivateFileFromCID(pt.fs, link.Name, link.Key, link.Cid)
		if err != nil {
			return nil, err
		}
		previousFile.SetContents(f)
		return previousFile.Put()
	}

	ch, err := NewPrivateFile(pt.fs, pt.info.Bnf, f)
	if err != nil {
		return nil, err
	}
	return ch.Put()
}

func (pt *PrivateTree) Put() (base.PutResult, error) {
	log.Debugw("PrivateTree.Put", "name", pt.name)
	store := pt.fs.DagStore()

	pt.ratchet.Add1()
	key := pt.ratchet.Key()
	pt.info.Ratchet = pt.ratchet.Encode()
	pt.info.Size = pt.info.Links.SizeSum()

	buf := &bytes.Buffer{}
	if err := cbor.NewEncoder(buf).Encode(pt.info); err != nil {
		return nil, err
	}

	res, err := store.PutEncryptedFile(base.NewMemfileReader("", buf), key[:])
	if err != nil {
		return nil, err
	}

	privName, err := pt.PrivateName()
	if err != nil {
		return nil, err
	}

	idBytes := CborByteArray(res.Cid.Bytes())
	if err := pt.fs.HAMT().Set(context.TODO(), string(privName), &idBytes); err != nil {
		return nil, err
	}

	pt.cid = res.Cid
	log.Debugw("PrivateTree.Put", "name", pt.name, "cid", pt.cid.String(), "size", pt.info.Size)
	return PutResult{
		PutResult: public.PutResult{
			Cid:  pt.cid,
			Size: pt.info.Size,
		},
		Key:     key,
		Pointer: privName,
	}, nil
}

func (pt *PrivateTree) updateUserlandLink(name string, res base.PutResult) {
	pt.info.Links.Add(res.(PutResult).ToPrivateLink(name))
	pt.info.Metadata.UnixMeta.Mtime = base.Timestamp().Unix()
}

func (pt *PrivateTree) removeUserlandLink(name string) {
	pt.info.Links.Remove(name)
	pt.info.Metadata.UnixMeta.Mtime = base.Timestamp().Unix()
}

type PrivateFile struct {
	fs      base.MerkleDagFS
	ratchet *SpiralRatchet
	name    string  // not persisted. used to implement fs.File interface
	cid     cid.Cid // cid header was loaded from. empty if new
	info    PrivateFileInfo
	content io.ReadCloser
}

type PrivateFileInfo struct {
	INumber        INumber
	Size           int64
	BareNamefilter BareNamefilter
	Ratchet        string
	Metadata       *base.Metadata
	ContentID      cid.Cid
}

var (
	_ DecryptedFile = (*PrivateFile)(nil)
	_ fs.File       = (*PrivateFile)(nil)
)

func NewPrivateFile(fs base.MerkleDagFS, parent BareNamefilter, f fs.File) (*PrivateFile, error) {
	in := NewINumber()
	bnf, err := NewBareNamefilter(parent, in)
	if err != nil {
		return nil, err
	}

	return &PrivateFile{
		fs:      fs,
		ratchet: NewSpiralRatchet(),
		content: f,
		info: PrivateFileInfo{
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

func LoadPrivateFileFromCID(fs base.MerkleDagFS, name string, key Key, id cid.Cid) (*PrivateFile, error) {
	store := fs.DagStore()
	f, err := store.GetEncryptedFile(id, key[:])
	if err != nil {
		log.Debugw("LoadPrivateFileFromCID", "err", err)
		return nil, err
	}

	info := PrivateFileInfo{}
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

	return &PrivateFile{
		fs:      fs,
		ratchet: ratchet,
		name:    name,
		cid:     id,
		info:    info,
		content: content,
	}, nil
}

func (pf *PrivateFile) BareNamefilter() BareNamefilter { return pf.info.BareNamefilter }
func (pf *PrivateFile) INumber() INumber               { return pf.info.INumber }
func (pf *PrivateFile) Cid() cid.Cid                   { return pf.cid }
func (pf *PrivateFile) Content() cid.Cid               { return pf.info.ContentID }

func (pf *PrivateFile) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		// TODO(b5): finish
	}
}

func (pf *PrivateFile) PrivateName() (PrivateName, error) {
	knf, err := AddKey(pf.info.BareNamefilter, pf.ratchet.Key())
	if err != nil {
		return "", err
	}
	return ToPrivateName(knf)
}
func (pf *PrivateFile) Key() Key { return pf.ratchet.Key() }

func (pf *PrivateFile) Stat() (fs.FileInfo, error) {
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

func (pf *PrivateFile) Read(p []byte) (n int, err error) { return pf.content.Read(p) }
func (pf *PrivateFile) Close() error                     { return pf.content.Close() }

func (pf *PrivateFile) SetContents(f fs.File) {
	pf.content = f
}

func (pf *PrivateFile) Put() (PutResult, error) {
	store := pf.fs.DagStore()

	// generate a new version key by advancing the ratchet
	// TODO(b5): what happens if anything errors after advancing the ratchet?
	// assuming we need to make a point of throwing away the file & cleaning the MMPT
	pf.ratchet.Add1()
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
	privName, err := pf.PrivateName()
	if err != nil {
		return PutResult{}, err
	}

	idBytes := CborByteArray(headerRes.Cid.Bytes())
	if err := pf.fs.HAMT().Set(context.TODO(), string(privName), &idBytes); err != nil {
		return PutResult{}, err
	}

	log.Debugw("PrivateFile.Put", "name", pf.name, "cid", pf.cid.String(), "size", res.Size)
	pf.cid = headerRes.Cid
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
	Pointer PrivateName
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
	Pointer PrivateName
}

func (r PutResult) ToPrivateLink(name string) PrivateLink {
	return PrivateLink{
		Link:    r.ToLink(name),
		Key:     r.Key,
		Pointer: r.Pointer,
	}
}
