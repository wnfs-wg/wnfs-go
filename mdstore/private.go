package mdstore

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"
	"io/fs"

	blockservice "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	cidutil "github.com/ipfs/go-cidutil"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	merkledag "github.com/ipfs/go-merkledag"
	balanced "github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	mh "github.com/multiformats/go-multihash"
	cipherchunker "github.com/qri-io/wnfs-go/cipherchunker"
	cipherfile "github.com/qri-io/wnfs-go/cipherfile"
	"github.com/qri-io/wnfs-go/ratchet"
)

type PrivateStore interface {
	PutEncryptedFile(f fs.File, key []byte) (PutResult, error)
	GetEncryptedFile(root cid.Cid, key []byte) (io.ReadCloser, error)
	Blockservice() blockservice.BlockService
	RatchetStore() ratchet.Store
}

func newAESGCMCipher(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	return cipher.NewGCM(block)
}

// warning! cipherStore doesn't pin!
type cipherStore struct {
	ctx   context.Context
	bserv blockservice.BlockService
	dag   ipld.DAGService
	rs    ratchet.Store
}

var _ PrivateStore = (*cipherStore)(nil)

func NewPrivateStore(ctx context.Context, bserv blockservice.BlockService, rs ratchet.Store) (PrivateStore, error) {
	return &cipherStore{
		ctx:   ctx,
		bserv: bserv,
		dag:   merkledag.NewDAGService(bserv),
		rs:    rs,
	}, nil
}

func (cs *cipherStore) Blockservice() blockservice.BlockService { return cs.bserv }

func (cs *cipherStore) GetEncryptedFile(root cid.Cid, key []byte) (io.ReadCloser, error) {
	auth, err := newAESGCMCipher(key)
	if err != nil {
		return nil, err
	}

	ses := dag.NewSession(cs.ctx, cs.dag)

	nd, err := ses.Get(cs.ctx, root)
	if err != nil {
		return nil, err
	}

	cf, err := cipherfile.NewCipherFile(cs.ctx, dag.NewReadOnlyDagService(ses), nd, auth)
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
		Cid:  nd.Cid(),
		Size: sr.Size(),
	}, nil
}

func (cs *cipherStore) RatchetStore() ratchet.Store { return cs.rs }

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
