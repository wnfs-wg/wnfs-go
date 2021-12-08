package private

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"

	hamt "github.com/filecoin-project/go-hamt-ipld/v3"
	blockservice "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	cidutil "github.com/ipfs/go-cidutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	merkledag "github.com/ipfs/go-merkledag"
	balanced "github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	mh "github.com/multiformats/go-multihash"
	base "github.com/qri-io/wnfs-go/base"
	cipherchunker "github.com/qri-io/wnfs-go/private/cipherchunker"
	cipherfile "github.com/qri-io/wnfs-go/private/cipherfile"
	ratchet "github.com/qri-io/wnfs-go/private/ratchet"
	public "github.com/qri-io/wnfs-go/public"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type Store interface {
	PutEncryptedFile(f fs.File, key []byte) (PutResult, error)
	GetEncryptedFile(root cid.Cid, key []byte) (io.ReadCloser, error)

	HAMT() *HAMT
	DAGService() ipld.DAGService
	Blockservice() blockservice.BlockService
	RatchetStore() ratchet.Store
}

func NodeStore(n base.Node) (Store, error) {
	st, err := n.Stat()
	if err != nil {
		return nil, err
	}
	mdfs, ok := st.Sys().(Store)
	if !ok {
		return nil, fmt.Errorf("node Sys is not a private.Store")
	}
	return mdfs, nil
}

// warning! cipherStore doesn't pin!
type cipherStore struct {
	ctx   context.Context
	bserv blockservice.BlockService
	dag   ipld.DAGService
	hamt  *HAMT
	rs    ratchet.Store
}

var _ Store = (*cipherStore)(nil)

func NewStore(ctx context.Context, bserv blockservice.BlockService, rs ratchet.Store) (Store, error) {
	h, err := NewEmptyHamt(bserv.Blockstore())
	if err != nil {
		return nil, err
	}

	return &cipherStore{
		ctx:   ctx,
		bserv: bserv,
		dag:   merkledag.NewDAGService(bserv),
		hamt:  h,
		rs:    rs,
	}, nil
}

func LoadStore(ctx context.Context, bserv blockservice.BlockService, rs ratchet.Store, hamtCid cid.Cid) (s Store, err error) {
	var h *HAMT
	if hamtCid.Defined() {
		if h, err = LoadHAMT(ctx, bserv.Blockstore(), hamtCid); err != nil {
			return nil, err
		}
	} else {
		if h, err = NewEmptyHamt(bserv.Blockstore()); err != nil {
			return nil, err
		}
	}

	return &cipherStore{
		ctx:   ctx,
		bserv: bserv,
		dag:   merkledag.NewDAGService(bserv),
		hamt:  h,
		rs:    rs,
	}, nil
}

func (cs *cipherStore) DAGService() ipld.DAGService             { return cs.dag }
func (cs *cipherStore) Blockservice() blockservice.BlockService { return cs.bserv }
func (cs *cipherStore) HAMT() *HAMT                             { return cs.hamt }
func (cs *cipherStore) RatchetStore() ratchet.Store             { return cs.rs }

func (cs *cipherStore) GetEncryptedFile(root cid.Cid, key []byte) (io.ReadCloser, error) {
	auth, err := newAESGCMCipher(key)
	if err != nil {
		return nil, err
	}

	ses := merkledag.NewSession(cs.ctx, cs.dag)

	nd, err := ses.Get(cs.ctx, root)
	if err != nil {
		return nil, fmt.Errorf("getting cid %s: %w", root, err)
	}

	cf, err := cipherfile.NewCipherFile(cs.ctx, merkledag.NewReadOnlyDagService(ses), nd, auth)
	if err != nil {
		return nil, err
	}
	return cf.(io.ReadCloser), nil
}

func (cs *cipherStore) PutEncryptedFile(f fs.File, key []byte) (PutResult, error) {
	fi, err := f.Stat()
	if err != nil {
		return PutResult{}, err
	}

	if fi.IsDir() {
		return PutResult{}, fmt.Errorf("cannot write encrypted directories")
	}

	auth, err := newAESGCMCipher(key)
	if err != nil {
		return PutResult{}, err
	}

	sr := &sizeReader{r: f}

	nd, err := cs.putEncryptedFile(sr, auth)
	if err != nil {
		return PutResult{}, err
	}

	return PutResult{
		PutResult: public.PutResult{
			Cid:  nd.Cid(),
			Size: sr.Size(),
		},
	}, nil
}

func (cs *cipherStore) putEncryptedFile(r io.Reader, auth cipher.AEAD) (ipld.Node, error) {
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return nil, err
	}
	prefix.MhType = mh.SHA2_256

	spl, err := cipherchunker.NewCipherSplitter(r, auth, 1024*256)
	if err != nil {
		return nil, err
	}

	dbp := ihelper.DagBuilderParams{
		Maxlinks:  1024,
		RawLeaves: true,

		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   32,
		},

		Dagserv: cs.dag,
	}

	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}

	return balanced.Layout(db)
}

func newCipher(key []byte) (cipher.AEAD, error) {
	return newAESGCMCipher(key)
}

func newAESGCMCipher(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	return cipher.NewGCM(block)
}

type sizeReader struct {
	size int
	r    io.Reader
}

func (lr *sizeReader) Read(p []byte) (int, error) {
	n, err := lr.r.Read(p)
	lr.size += n
	return n, err
}

func (lr *sizeReader) Size() int64 { return int64(lr.size) }

type HAMT struct {
	cid   cid.Cid
	root  *hamt.Node
	store *ipldcbor.BasicIpldStore
}

func NewEmptyHamt(bstore blockstore.Blockstore) (*HAMT, error) {
	store := ipldcbor.NewCborStore(bstore)
	hamtRoot, err := hamt.NewNode(store)
	if err != nil {
		return nil, err
	}
	return &HAMT{
		cid:   cid.Undef,
		root:  hamtRoot,
		store: store,
	}, nil
}

func LoadHAMT(ctx context.Context, bstore blockstore.Blockstore, id cid.Cid) (*HAMT, error) {
	store := ipldcbor.NewCborStore(bstore)
	root, err := hamt.LoadNode(ctx, store, id)
	if err != nil {
		log.Debugw("LoadHAMT", "cid", id, "err", err)
		return nil, err
	}

	log.Debugw("LoadHAMT", "cid", id)
	return &HAMT{
		cid:   id,
		root:  root,
		store: store,
	}, nil
}

func (h *HAMT) CID() cid.Cid     { return h.cid }
func (h *HAMT) Root() *hamt.Node { return h.root }

func (h *HAMT) Write(ctx context.Context) error {
	id, err := h.root.Write(ctx)
	if err != nil {
		return err
	}
	id2, err := h.store.Put(ctx, h.root)
	if err != nil {
		return err
	}
	log.Debugw("put hamt root", "cid", id)

	if !id.Equals(id2) {
		return fmt.Errorf("unequal root IDs: %q != %q", id, id2)
	}
	h.cid = id
	return nil
}

func (h *HAMT) Merge(ctx context.Context, b *hamt.Node) error {
	return b.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		_, err := h.root.SetIfAbsent(ctx, k, val)
		return err
	})
}

func (h *HAMT) Diagnostic(ctx context.Context) map[string]string {
	vs := map[string]string{}
	h.root.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		vs[k] = base64.URLEncoding.EncodeToString(val.Raw)
		return nil
	})

	log.Debugw("HAMT.Diagnostic", "cid", h.cid, "len(values)", len(vs))
	return vs
}

// A CBOR-marshalable byte array.
type CborByteArray []byte

func (c *CborByteArray) MarshalCBOR(w io.Writer) error {
	if err := cbg.WriteMajorTypeHeader(w, cbg.MajByteString, uint64(len(*c))); err != nil {
		return err
	}
	_, err := w.Write(*c)
	return err
}

func (c *CborByteArray) UnmarshalCBOR(r io.Reader) error {
	maj, extra, err := cbg.CborReadHeader(r)
	if err != nil {
		return err
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}
	if uint64(cap(*c)) < extra {
		*c = make([]byte, extra)
	}
	if _, err := io.ReadFull(r, *c); err != nil {
		return err
	}
	return nil
}

// Copy blocks from src to dst
func CopyBlocks(ctx context.Context, id cid.Cid, src, dst Store) (err error) {
	var n ipld.Node
	// TODO(b5): need to do this switch on codec because go-merkledag package no
	// longer supports dag-cbor out of the box >:(
	// More reasons to switch away from go-ipld libraries
	switch id.Prefix().Codec {
	case 0x71: // dag-cbor
		blk, err := src.Blockservice().GetBlock(ctx, id)
		if err != nil {
			return err
		}
		n, err = ipldcbor.DecodeBlock(blk)
		if err != nil {
			return err
		}
	default:
		n, err = src.DAGService().Get(ctx, id)
		if err != nil {
			return fmt.Errorf("reading DAG node %s: %w", id, err)
		}
	}

	log.Debugw("CopyBlocks", "cid", id, "len(links)", len(n.Links()))
	for _, l := range n.Links() {
		if err := CopyBlocks(ctx, l.Cid, src, dst); err != nil {
			return fmt.Errorf("copying block %s: %w", l.Cid, err)
		}
	}

	return copyBlock(ctx, id, src, dst)
}

func copyBlock(ctx context.Context, id cid.Cid, src, dst Store) error {
	blk, err := src.Blockservice().Blockstore().Get(ctx, id)
	if err != nil {
		return err
	}
	return dst.Blockservice().Blockstore().Put(ctx, blk)
}

func MergeHAMTBlocks(ctx context.Context, src, dst Store) error {
	log.Debugw("Merging HAMTs", "src", src.HAMT().cid, "dst", dst.HAMT().cid)
	dstRoot := dst.HAMT().root

	err := src.HAMT().root.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		_, err := dstRoot.SetIfAbsent(ctx, k, val)
		if err != nil {
			return err
		}

		// if set {
		if _, id, err := cid.CidFromBytes(val.Raw[2:]); err == nil {
			return CopyBlocks(ctx, id, src, dst)
		}
		// }
		return nil
	})

	if err != nil {
		return err
	}

	copyBlock(ctx, src.HAMT().cid, src, dst)

	return dst.HAMT().Write(ctx)
}
