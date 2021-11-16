package private

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"sort"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	golog "github.com/ipfs/go-log"
	"github.com/multiformats/go-multihash"
	base "github.com/qri-io/wnfs-go/base"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
	public "github.com/qri-io/wnfs-go/public"
	ratchet "github.com/qri-io/wnfs-go/ratchet"
)

var log = golog.Logger("wnfs")

type Key [32]byte

func NewKey() Key {
	return ratchet.NewSpiral().Key()
}

func (k Key) Encode() string { return base64.URLEncoding.EncodeToString(k[:]) }

func (k *Key) Decode(s string) error {
	data, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	for i, d := range data {
		k[i] = d
	}
	return nil
}

func (k Key) IsEmpty() bool { return k == Key([32]byte{}) }

func (k Key) MarshalJSON() ([]byte, error) {
	return []byte(`"` + k.Encode() + `"`), nil
}

func (k *Key) UnmarshalJSON(d []byte) error {
	var s string
	if err := json.Unmarshal(d, &s); err != nil {
		return err
	}
	return k.Decode(s)
}

type Info interface {
	base.FileInfo
	Ratchet() *ratchet.Spiral
	PrivateName() (Name, error)
}

func Stat(f fs.File) (Info, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	pi, ok := fi.(Info)
	if !ok {
		return nil, fmt.Errorf("file %q doesn't contain private info", fi.Name())
	}
	return pi, nil
}

type privateNode interface {
	base.Node

	INumber() INumber
	Ratchet() *ratchet.Spiral
	PrivateName() (Name, error)
	BareNamefilter() BareNamefilter
}

type privateTree interface {
	privateNode
	base.Tree
}

type Root struct {
	*Tree
	ctx context.Context
}

var (
	_ privateTree    = (*Root)(nil)
	_ fs.File        = (*Root)(nil)
	_ fs.ReadDirFile = (*Root)(nil)
)

func NewEmptyRoot(ctx context.Context, store Store, name string, rootKey Key) (*Root, error) {
	private, err := NewEmptyTree(store, IdentityBareNamefilter(), name)
	if err != nil {
		return nil, err
	}
	return &Root{
		ctx:  ctx,
		Tree: private,
	}, nil
}

func LoadRoot(ctx context.Context, store Store, name string, rootKey Key, rootName Name) (*Root, error) {
	if rootName == Name("") {
		return nil, fmt.Errorf("privateName is required")
	}

	data := CborByteArray{}
	exists, err := store.HAMT().Root().Find(ctx, string(rootName), &data)
	if err != nil {
		log.Debugw("LoadRoot find root name in HAMT", "name", string(rootName), "err", err)
		return nil, fmt.Errorf("opening private root: %w", err)
	} else if !exists {
		log.Debugw("LoadRoot: rootName not found in HAMT", "name", string(rootName))
		return nil, fmt.Errorf("opening private root: %w", base.ErrNotFound)
	}
	_, privateRoot, err := cid.CidFromBytes([]byte(data))
	if err != nil {
		return nil, fmt.Errorf("reading CID bytes: %w", err)
	}

	tree, err := LoadTree(store, name, rootKey, privateRoot)
	if err != nil {
		return nil, err
	}
	return &Root{
		ctx:  ctx,
		Tree: tree,
	}, nil
}

func (r *Root) Context() context.Context { return r.ctx }
func (r *Root) Cid() cid.Cid {
	if r.fs.HAMT() == nil {
		return cid.Undef
	}
	return r.fs.HAMT().CID()
}
func (r *Root) HAMTCid() *cid.Cid {
	id := r.Cid()
	return &id
}

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
	return res, r.putRoot()
}

func (r *Root) Copy(path base.Path, srcPathStr string, srcFS fs.FS) (res base.PutResult, err error) {
	res, err = r.Tree.Copy(path, srcPathStr, srcFS)
	if err != nil {
		return nil, err
	}
	return res, r.putRoot()
}

func (r *Root) Rm(path base.Path) (base.PutResult, error) {
	res, err := r.Tree.Rm(path)
	if err != nil {
		return nil, err
	}
	return res, r.putRoot()
}

func (r *Root) Mkdir(path base.Path) (res base.PutResult, err error) {
	res, err = r.Tree.Mkdir(path)
	if err != nil {
		return nil, err
	}
	return res, r.putRoot()
}

func (r *Root) Put() (base.PutResult, error) {
	ctx := context.TODO()
	log.Debugw("Root.Put", "name", r.name, "hamtCID", r.fs.HAMT().CID(), "key", Key(r.ratchet.Key()).Encode())

	// TODO(b5): note entirely sure this is necessary
	if _, err := r.fs.RatchetStore().PutRatchet(ctx, r.header.Info.INumber.Encode(), r.ratchet); err != nil {
		return nil, err
	}

	res, err := r.Tree.Put()
	if err != nil {
		return nil, err
	}
	return res, r.putRoot()
}

func (r *Root) putRoot() error {
	ctx := context.TODO()
	if r.fs.HAMT() != nil {
		if err := r.fs.HAMT().Write(ctx); err != nil {
			return err
		}
	}
	pn, err := r.PrivateName()
	if err != nil {
		return err
	}
	log.Debugw("putRoot", "privateName", string(pn), "name", r.name, "hamtCID", r.fs.HAMT().CID(), "key", Key(r.ratchet.Key()).Encode())
	return r.fs.RatchetStore().Flush()
}

type Tree struct {
	fs   Store
	name string  // not stored on the node. used to satisfy fs.File interface
	cid  cid.Cid // header node cid this tree was loaded from. empty if unstored

	header  Header
	ratchet *ratchet.Spiral
	links   PrivateLinks
}

var (
	_ privateTree    = (*Tree)(nil)
	_ Info           = (*Tree)(nil)
	_ fs.File        = (*Tree)(nil)
	_ fs.ReadDirFile = (*Tree)(nil)
)

func NewEmptyTree(fs Store, parent BareNamefilter, name string) (*Tree, error) {
	in := NewINumber()
	bnf, err := NewBareNamefilter(parent, in)
	if err != nil {
		return nil, err
	}

	ts := base.Timestamp()

	return &Tree{
		fs:      fs,
		ratchet: ratchet.NewSpiral(),
		name:    name,
		header: Header{
			Info: HeaderInfo{
				WNFS:  base.LatestVersion,
				Type:  base.NTDir,
				Mode:  base.ModeDefault,
				Ctime: ts.Unix(),
				Mtime: ts.Unix(),

				INumber:        in,
				BareNamefilter: bnf,
			},
		},
		links: PrivateLinks{},
	}, nil
}

func LoadTree(fs Store, name string, key Key, id cid.Cid) (*Tree, error) {
	log.Debugw("LoadTree", "name", name, "cid", id)
	ctx := context.TODO()

	header, err := loadHeader(ctx, fs, key, id)
	if err != nil {
		return nil, err
	}

	blk, err := fs.Blockservice().GetBlock(ctx, header.ContentID)
	if err != nil {
		return nil, err
	}
	links, err := unmarshalPrivateLinksBlock(blk, key)
	if err != nil {
		return nil, err
	}

	ratchet, err := ratchet.DecodeSpiral(header.Info.Ratchet)
	if err != nil {
		return nil, fmt.Errorf("decoding ratchet: %w", err)
	}
	header.Info.Ratchet = ""

	return &Tree{
		fs:      fs,
		name:    name,
		ratchet: ratchet,
		cid:     id,
		header:  header,
		links:   links,
	}, nil
}

func LoadTreeFromName(ctx context.Context, fs Store, key Key, name string, pn Name) (*Tree, error) {
	id, err := cidFromPrivateName(ctx, fs, pn)
	if err != nil {
		return nil, err
	}
	return LoadTree(fs, name, key, id)
}

func (pt *Tree) Name() string                   { return pt.name }
func (pt *Tree) Size() int64                    { return pt.header.Info.Size }
func (pt *Tree) ModTime() time.Time             { return time.Unix(pt.header.Info.Mtime, 0) }
func (pt *Tree) Mode() fs.FileMode              { return fs.FileMode(pt.header.Info.Ctime) }
func (pt *Tree) IsDir() bool                    { return true }
func (pt *Tree) Sys() interface{}               { return pt.fs }
func (pt *Tree) Stat() (fs.FileInfo, error)     { return pt, nil }
func (pt *Tree) Cid() cid.Cid                   { return pt.cid }
func (pt *Tree) INumber() INumber               { return pt.header.Info.INumber }
func (pt *Tree) Ratchet() *ratchet.Spiral       { return pt.ratchet }
func (pt *Tree) BareNamefilter() BareNamefilter { return pt.header.Info.BareNamefilter }
func (pt *Tree) PrivateFS() Store               { return pt.fs }
func (pt *Tree) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		Cid: pt.cid,
		// TODO(b5):
		// Previous: prevCID(pt.fs, pt.ratchet, pt.header.Info.BareNamefilter),
		// Metadata: pt.info.Metadata,
		// Size:     pt.info.Size,
	}
}

func (pt *Tree) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   pt.name,
		Cid:    pt.cid,
		Size:   pt.header.Info.Size,
		IsFile: false,
		Mtime:  pt.header.Info.Mtime,
	}
}

func (pt *Tree) PrivateName() (Name, error) {
	knf, err := AddKey(pt.header.Info.BareNamefilter, Key(pt.ratchet.Key()))
	if err != nil {
		return "", err
	}
	return ToName(knf)
}
func (pt *Tree) Key() Key { return pt.ratchet.Key() }

func (pt *Tree) Read(p []byte) (n int, err error) {
	return -1, fmt.Errorf("cannot read directory")
}
func (pt *Tree) Close() error { return nil }

func (pt *Tree) ReadDir(n int) ([]fs.DirEntry, error) {
	if n < 0 {
		n = len(pt.links)
	}

	entries := make([]fs.DirEntry, 0, n)
	for i, link := range pt.links.SortedSlice() {
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

	link := pt.links.Get(head)
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
		link := pt.links.Get(head)
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

func (pt *Tree) History(ctx context.Context, maxRevs int) ([]base.HistoryEntry, error) {
	return history(ctx, pt, maxRevs)
}

func history(ctx context.Context, n privateNode, maxRevs int) ([]base.HistoryEntry, error) {
	st, err := n.Stat()
	if err != nil {
		return nil, err
	}

	bnf := n.BareNamefilter()
	store, err := NodeStore(n)
	if err != nil {
		return nil, err
	}

	old, err := store.RatchetStore().OldestKnownRatchet(ctx, n.INumber().Encode())
	if err != nil {
		log.Debugw("getting oldest known ratchet", "err", err)
		return nil, err
	}
	if old == nil {
		log.Debugw("getting oldest known ratchet", "err", err)
		return nil, err
	}

	recent := n.Ratchet()
	ratchets, err := recent.Previous(old, maxRevs)
	if err != nil {
		log.Debugw("history previous revs", "err", err)
		return nil, err
	}
	ratchets = append([]*ratchet.Spiral{recent}, ratchets...) // add current revision to top of stack

	log.Debugw("History", "name", st.Name(), "len(ratchets)", len(ratchets), "oldest_ratchet", old.Encode())

	hist := make([]base.HistoryEntry, len(ratchets))
	for i, rcht := range ratchets {
		key := Key(rcht.Key())
		knf, err := AddKey(bnf, key)
		if err != nil {
			return nil, err
		}
		pn, err := ToName(knf)
		if err != nil {
			return nil, err
		}
		headerID, err := cidFromPrivateName(ctx, store, pn)
		if err != nil {
			log.Debugw("getting CID from private name", "err", err)
			return nil, err
		}

		// f, err := store.GetEncryptedFile(contentID, key[:])
		// if err != nil {
		// 	log.Debugw("LoadTree", "err", err)
		// 	return nil, err
		// }
		// defer f.Close()

		// // TODO(b5): using TreeInfo for both files & directories
		// // info := TreeInfo{}
		// // if err := cbor.NewDecoder(f).Decode(&info); err != nil {
		// // 	log.Debugw("LoadTree", "err", err)
		// // 	return nil, err
		// // }
		header, err := loadHeader(ctx, store, key, headerID)
		if err != nil {
			log.Debugw("loading historical header", "cid", headerID, "err", err)
		}

		hist[i] = base.HistoryEntry{
			Cid: headerID,
			// Metadata: info.Metadata,
			Size: header.Info.Size,

			Key:         key.Encode(),
			PrivateName: string(pn),
		}
	}

	log.Debugw("found history", "len(hist)", len(hist))
	return hist, nil
}

func (pt *Tree) getOrCreateDirectChildTree(name string) (*Tree, error) {
	link := pt.links.Get(name)
	if link == nil {
		return NewEmptyTree(pt.fs, pt.header.Info.BareNamefilter, name)
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
	if link := pt.links.Get(name); link != nil {
		tree, err = LoadTree(pt.fs, link.Name, link.Key, link.Cid)
		if err != nil {
			return nil, err
		}
	} else {
		tree, err = NewEmptyTree(pt.fs, pt.header.Info.BareNamefilter, name)
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
	if link := pt.links.Get(name); link != nil {
		previousFile, err := LoadFileFromCID(pt.fs, link.Name, link.Key, link.Cid)
		if err != nil {
			return nil, err
		}
		previousFile.SetContents(f)
		return previousFile.Put()
	}

	ch, err := NewFile(pt.fs, pt.header.Info.BareNamefilter, f)
	if err != nil {
		return nil, err
	}
	return ch.Put()
}

func (pt *Tree) Put() (base.PutResult, error) {
	ctx := context.TODO()
	log.Debugw("Tree.Put", "name", pt.name, "len(links)", len(pt.links), "prevRatchet", pt.ratchet.Summary())

	pt.ratchet.Inc()
	log.Debugw("Tree.Put", "new ratchet", pt.ratchet.Summary())
	key := pt.ratchet.Key()
	pt.header.Info.Ratchet = pt.ratchet.Encode()
	pt.header.Info.Size = pt.links.SizeSum()

	linksBlk, err := pt.links.marshalEncryptedBlock(key)
	if err != nil {
		return nil, err
	}
	pt.header.ContentID = linksBlk.Cid()

	blk, err := pt.header.encryptHeaderBlock(key)
	if err != nil {
		return nil, err
	}

	if err = pt.fs.Blockservice().Blockstore().PutMany([]blocks.Block{blk, linksBlk}); err != nil {
		return nil, err
	}
	pt.cid = blk.Cid()

	privName, err := pt.PrivateName()
	if err != nil {
		return nil, err
	}

	if _, err = pt.fs.RatchetStore().PutRatchet(ctx, pt.header.Info.INumber.Encode(), pt.ratchet); err != nil {
		return nil, err
	}

	idBytes := CborByteArray(pt.cid.Bytes())
	if err := pt.fs.HAMT().Root().Set(ctx, string(privName), &idBytes); err != nil {
		return nil, err
	}

	log.Debugw("Tree.Put", "name", pt.name, "privateName", string(privName), "cid", pt.cid.String(), "size", pt.header.Info.Size)
	return PutResult{
		PutResult: public.PutResult{
			Cid:  pt.cid,
			Size: pt.header.Info.Size,
		},
		Key:     key,
		Pointer: privName,
	}, nil
}

func (pt *Tree) updateUserlandLink(name string, res base.PutResult) {
	pt.links.Add(res.(PutResult).ToPrivateLink(name))
	pt.header.Info.Mtime = base.Timestamp().Unix()
}

func (pt *Tree) removeUserlandLink(name string) {
	pt.links.Remove(name)
	pt.header.Info.Mtime = base.Timestamp().Unix()
}

type File struct {
	fs     Store
	name   string  // not persisted. used to implement fs.File interface
	cid    cid.Cid // cid header was loaded from. empty if new
	header Header

	ratchet *ratchet.Spiral
	content io.ReadCloser
}

var (
	_ privateNode = (*File)(nil)
	_ fs.File     = (*File)(nil)
	_ Info        = (*File)(nil)
)

func NewFile(fs Store, parent BareNamefilter, f fs.File) (*File, error) {
	in := NewINumber()
	bnf, err := NewBareNamefilter(parent, in)
	if err != nil {
		return nil, err
	}

	md := base.NewUnixMeta(true)

	return &File{
		fs:      fs,
		ratchet: ratchet.NewSpiral(),
		content: f,
		header: Header{
			Info: HeaderInfo{
				WNFS:  base.LatestVersion,
				Type:  base.NTFile,
				Mode:  base.ModeDefault,
				Ctime: md.Ctime,
				Mtime: md.Mtime,
				Size:  -1,

				INumber:        in,
				BareNamefilter: bnf,
			},
		},
	}, nil
}

func LoadFileFromCID(store Store, name string, key Key, id cid.Cid) (*File, error) {
	log.Debugw("LoadFileFromCID", "name", name, "cid", id, "key", key.Encode())
	header, err := loadHeader(context.TODO(), store, key, id)
	if err != nil {
		log.Debugw("LoadFileFromCID", "err", err)
		return nil, fmt.Errorf("decoding s-node %q header: %w", name, err)
	}

	ratchet, err := ratchet.DecodeSpiral(header.Info.Ratchet)
	if err != nil {
		return nil, err
	}
	header.Info.Ratchet = ""

	// TODO(b5): lazy-load on first call to Read()
	content, err := store.GetEncryptedFile(header.ContentID, key[:])
	if err != nil {
		return nil, fmt.Errorf("decoding s-node %q file: %w", name, err)
	}

	return &File{
		fs:      store,
		ratchet: ratchet,
		name:    name,
		cid:     id,
		header:  header,
		content: content,
	}, nil
}

func (pf *File) Ratchet() *ratchet.Spiral       { return pf.ratchet }
func (pf *File) BareNamefilter() BareNamefilter { return pf.header.Info.BareNamefilter }
func (pf *File) INumber() INumber               { return pf.header.Info.INumber }
func (pf *File) Cid() cid.Cid                   { return pf.cid }
func (pf *File) Content() cid.Cid               { return pf.header.ContentID }
func (pf *File) PrivateFS() Store               { return pf.fs }
func (pf *File) IsDir() bool                    { return false }
func (pf *File) ModTime() time.Time             { return time.Unix(pf.header.Info.Mtime, 0) }
func (pf *File) Mode() fs.FileMode              { return fs.FileMode(pf.header.Info.Mode) }
func (pf *File) Name() string                   { return pf.name }
func (pf *File) Size() int64                    { return pf.header.Info.Size }
func (pf *File) Sys() interface{}               { return pf.fs }
func (pf *File) Stat() (fs.FileInfo, error)     { return pf, nil }

func (pf *File) PrivateName() (Name, error) {
	knf, err := AddKey(pf.header.Info.BareNamefilter, Key(pf.ratchet.Key()))
	if err != nil {
		return "", err
	}
	return ToName(knf)
}

func (pf *File) AsHistoryEntry() base.HistoryEntry {
	return base.HistoryEntry{
		// TODO(b5): finish
	}
}

func (pf *File) AsLink() mdstore.Link {
	return mdstore.Link{
		Name:   pf.name,
		Cid:    pf.cid,
		Size:   pf.header.Info.Size,
		IsFile: true,
		Mtime:  pf.header.Info.Mtime,
	}
}

func (pf *File) Key() Key { return pf.ratchet.Key() }

func (pf *File) Read(p []byte) (n int, err error) { return pf.content.Read(p) }
func (pf *File) Close() error                     { return pf.content.Close() }

func (pf *File) History(ctx context.Context, maxRevs int) ([]base.HistoryEntry, error) {
	return history(ctx, pf, maxRevs)
}

func (pf *File) SetContents(f fs.File) {
	pf.content = f
}

// TODO(b5): this *might* not be necessary. need to test with remote node merge
func (pf *File) ensureContent() (err error) {
	if pf.content == nil {
		key := pf.ratchet.Key()
		pf.content, err = pf.fs.GetEncryptedFile(pf.cid, key[:])
	}
	return err
}

func (pf *File) Put() (PutResult, error) {
	ctx := context.TODO()
	store := pf.fs

	// generate a new version key by advancing the ratchet
	// TODO(b5): what happens if anything errors after advancing the ratchet?
	// assuming we need to make a point of throwing away the file & cleaning the HAMT
	pf.ratchet.Inc()
	key := pf.ratchet.Key()

	res, err := store.PutEncryptedFile(base.NewMemfileReader(pf.name, pf.content), key[:])
	if err != nil {
		return PutResult{}, err
	}

	// update header details
	pf.header.ContentID = res.Cid
	pf.header.Info.Size = res.Size
	pf.header.Info.Ratchet = pf.ratchet.Encode()
	pf.header.Info.Mtime = base.Timestamp().Unix()

	blk, err := pf.header.encryptHeaderBlock(key)
	if err != nil {
		return PutResult{}, err
	}

	if err := store.Blockservice().Blockstore().Put(blk); err != nil {
		return PutResult{}, err
	}
	pf.cid = blk.Cid()

	// create private name from key
	privName, err := pf.PrivateName()
	if err != nil {
		return PutResult{}, err
	}

	if _, err = store.RatchetStore().PutRatchet(ctx, pf.header.Info.INumber.Encode(), pf.ratchet); err != nil {
		return PutResult{}, err
	}

	idBytes := CborByteArray(pf.cid.Bytes())
	if err := pf.fs.HAMT().Root().Set(ctx, string(privName), &idBytes); err != nil {
		return PutResult{}, err
	}

	log.Debugw("File.Put", "name", pf.name, "cid", pf.cid.String(), "size", res.Size)
	return PutResult{
		PutResult: public.PutResult{
			Cid:      pf.cid,
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

func (n INumber) Encode() string { return base64.URLEncoding.EncodeToString(n[:]) }

type PrivateLink struct {
	mdstore.Link
	Key     Key
	Pointer Name
}

func loadNodeFromPrivateLink(fs Store, link PrivateLink) (privateNode, error) {
	if link.IsFile {
		return LoadFileFromCID(fs, link.Name, link.Key, link.Cid)
	}
	return LoadTree(fs, link.Name, link.Key, link.Cid)
}

type PrivateLinks map[string]PrivateLink

func unmarshalPrivateLinksBlock(blk blocks.Block, key Key) (PrivateLinks, error) {
	aead, err := newCipher(key[:])
	if err != nil {
		return nil, err
	}
	ciphertext := blk.RawData()
	plaintext, err := aead.Open(nil, ciphertext[:aead.NonceSize()], ciphertext[aead.NonceSize():], nil)
	if err != nil {
		return nil, err
	}

	links := PrivateLinks{}
	err = cbor.Unmarshal(plaintext, &links)
	return links, err
}

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

func (pls PrivateLinks) marshalEncryptedBlock(key Key) (blocks.Block, error) {
	plaintext, err := cbor.Marshal(pls)
	if err != nil {
		return nil, err
	}

	log.Debugw("encrypting private links", "key", key.Encode())
	aead, err := newCipher(key[:])
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	// TODO(b5): still using random nonces, switching to monotonic long-term
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, nil)
	data := append(nonce, ciphertext...)

	hash, err := multihash.Sum(data, multihash.SHA2_256, -1)

	return blocks.NewBlockWithCid(data, cid.NewCidV1(cid.Raw, hash))
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

func cidFromPrivateName(ctx context.Context, fs Store, pn Name) (id cid.Cid, err error) {
	exists, data, err := fs.HAMT().Root().FindRaw(ctx, string(pn))
	if err != nil {
		return id, err
	}
	if !exists {
		return id, base.ErrNotFound
	}

	// TODO(b5): lol wtf just plugged this 2 byte prefix strip in & CID parsing works,
	// figure out the proper way to decode cids out of the HAMT
	_, id, err = cid.CidFromBytes(data[2:])
	return id, err
}

type Header struct {
	Info      HeaderInfo
	Metadata  cid.Cid
	ContentID cid.Cid
}

type HeaderInfo struct {
	WNFS  base.SemVer
	Type  base.NodeType
	Mode  uint32
	Ctime int64
	Mtime int64
	Size  int64

	INumber        INumber
	BareNamefilter BareNamefilter
	Ratchet        string
}

func HeaderInfoFromCBOR(d []byte) (HeaderInfo, error) {
	hi := HeaderInfo{}
	err := base.DecodeCBOR(d, &hi)
	return hi, err
}

func (hi HeaderInfo) CBOR() (*bytes.Buffer, error) {
	return base.EncodeCBOR(hi)
}

func (h Header) encryptHeaderBlock(key Key) (blocks.Block, error) {
	buf, err := h.Info.CBOR()
	if err != nil {
		return nil, err
	}

	log.Debugw("encrypting header info block", "key", key.Encode())
	aead, err := newCipher(key[:])
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	// TODO(b5): still using random nonces, switching to monotonic long-term
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	encrypted := aead.Seal(nil, nonce, buf.Bytes(), nil)
	header := map[string]interface{}{
		"info":    append(nonce, encrypted...),
		"content": h.ContentID,
	}
	log.Debugw("content", "cid", h.ContentID)
	if !h.Metadata.Equals(cid.Undef) {
		header["metadata"] = h.Metadata
	}
	return cbornode.WrapObject(header, multihash.SHA2_256, -1)
}

func loadHeader(ctx context.Context, s Store, key Key, id cid.Cid) (h Header, err error) {
	log.Debugw("decrypting header block", "cid", id, "key", key.Encode())
	blk, err := s.Blockservice().GetBlock(ctx, id)
	if err != nil {
		return h, fmt.Errorf("getting header block %q: %w", id.String(), err)
	}

	return decodeHeaderBlock(blk, key)
}

func decodeHeaderBlock(blk blocks.Block, key Key) (h Header, err error) {
	env := map[string]interface{}{}
	if err := cbor.Unmarshal(blk.RawData(), &env); err != nil {
		return h, err
	}

	encInfo, ok := env["info"].([]byte)
	if !ok {
		return h, fmt.Errorf("header is missing info field")
	}

	log.Debugw("content", "type", fmt.Sprintf("%T", env["content"].(cbor.Tag).Content), "id", env["content"])
	if content, ok := env["content"]; ok {
		if h.ContentID, err = cidFromCBORTag(content); err != nil {
			return h, err
		}
	} else {
		return h, fmt.Errorf("header has no content cid")
	}

	aead, err := newCipher(key[:])
	if err != nil {
		return h, err
	}

	plaintext, err := aead.Open(nil, encInfo[:aead.NonceSize()], encInfo[aead.NonceSize():], nil)
	if err != nil {
		return h, fmt.Errorf("decrypting info: %w", err)
	}

	if h.Info, err = HeaderInfoFromCBOR(plaintext); err != nil {
		return h, err
	}

	return h, nil
}

func cidFromCBORTag(v interface{}) (cid.Cid, error) {
	t, ok := v.(cbor.Tag)
	if !ok {
		return cid.Undef, fmt.Errorf("expected value to be a cbor.Tag")
	}
	d, ok := t.Content.([]byte)
	if !ok {
		return cid.Undef, fmt.Errorf("expected tag contents to be bytes")
	}
	return cid.Cast(d[1:])
}
