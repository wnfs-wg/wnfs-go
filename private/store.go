package private

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
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	merkledag "github.com/ipfs/go-merkledag"
	balanced "github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	mh "github.com/multiformats/go-multihash"
	cipherchunker "github.com/qri-io/wnfs-go/ipfs/cipherchunker"
	cipherfile "github.com/qri-io/wnfs-go/ipfs/cipherfile"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
)

func newAESGCMCipher(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	return cipher.NewGCM(block)
}

// warning! cipherStore doesn't pin!
type cipherStore struct {
	ctx context.Context
	bs  blockstore.Blockstore
	dag ipld.DAGService
}

var _ mdstore.PrivateStore = (*cipherStore)(nil)

func NewStore(ctx context.Context, bs blockstore.Blockstore) (mdstore.PrivateStore, error) {
	return &cipherStore{
		ctx: ctx,
		bs:  bs,
		dag: merkledag.NewDAGService(blockservice.New(bs, nil)),
	}, nil
}

func (cs *cipherStore) Blockstore() blockstore.Blockstore { return cs.bs }

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

func (cs *cipherStore) PutEncryptedFile(f fs.File, key []byte) (mdstore.PutResult, error) {
	fi, err := f.Stat()
	if err != nil {
		return mdstore.PutResult{}, err
	}

	if fi.IsDir() {
		return mdstore.PutResult{}, fmt.Errorf("cannot write encrypted directories")
	}

	auth, err := newAESGCMCipher(key)
	if err != nil {
		return mdstore.PutResult{}, err
	}

	nd, err := cs.putEncryptedFile(f, auth)
	if err != nil {
		return mdstore.PutResult{}, err
	}

	return mdstore.PutResult{
		Cid:  nd.Cid(),
		Size: fi.Size(),
	}, nil
}

func (cs *cipherStore) putEncryptedFile(f fs.File, auth cipher.AEAD) (ipld.Node, error) {
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return nil, err
	}
	prefix.MhType = mh.SHA2_256

	spl, err := cipherchunker.NewCipherSplitter(f, auth, 1024*1024)
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
