package ipfs

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"io"
	"io/fs"

	blockservice "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-filestore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	cipherchunker "github.com/qri-io/wnfs-go/cipherchunker"
	cipherfile "github.com/qri-io/wnfs-go/cipherfile"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
)

func newAESGCMCipher(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	return cipher.NewGCM(block)
}

func (fs *Filestore) GetEncryptedFile(ctx context.Context, root cid.Cid, key []byte) (io.ReadCloser, error) {
	auth, err := newAESGCMCipher(key)
	if err != nil {
		return nil, err
	}

	ses := dag.NewSession(ctx, fs.node.DAG)
	nd, err := ses.Get(ctx, root)
	if err != nil {
		return nil, err
	}

	cf, err := cipherfile.NewCipherFile(ctx, dag.NewReadOnlyDagService(ses), nd, auth)
	if err != nil {
		return nil, err
	}
	return cf.(io.ReadCloser), nil
}

func (fs *Filestore) PutEncryptedFile(f fs.File, key []byte) (mdstore.PutResult, error) {
	auth, err := newAESGCMCipher(key)
	if err != nil {
		return mdstore.PutResult{}, err
	}

	id, err := fs.putEncryptedFile(f, auth)
	if err != nil {
		return mdstore.PutResult{}, err
	}

	// storedFile, err := fs.capi.Unixfs().Get(fs.ctx, corepath.IpfsPath(id))
	// if err != nil {
	// 	return mdstore.PutResult{}, err
	// }

	// size, err := storedFile.Size()
	// if err != nil {
	// 	return mdstore.PutResult{}, err
	// }

	return mdstore.PutResult{
		Cid:  id,
		Size: 1,
	}, nil
}

func (fs *Filestore) putEncryptedFile(f fs.File, auth cipher.AEAD) (cid.Cid, error) {
	addblockstore := fs.node.Blockstore
	exch := fs.node.Exchange
	pinning := fs.node.Pinning
	bserv := blockservice.New(addblockstore, exch) // hash security 001
	dserv := dag.NewDAGService(bserv)

	// add a sync call to the DagService
	// this ensures that data written to the DagService is persisted to the underlying datastore
	// TODO: propagate the Sync function from the datastore through the blockstore, blockservice and dagservice
	syncDserv := &syncDagService{
		DAGService: dserv,
		syncFn: func() error {
			ds := fs.node.Repo.Datastore()
			if err := ds.Sync(blockstore.BlockPrefix); err != nil {
				return err
			}
			return ds.Sync(filestore.FilestorePrefix)
		},
	}

	fileAdder, err := cipherchunker.NewAdder(fs.ctx, addblockstore, pinning, syncDserv, auth)
	if err != nil {
		return cid.Undef, err
	}

	// fileAdder.Chunker = settings.Chunker
	// if settings.Events != nil {
	// 	fileAdder.Out = settings.Events
	// 	fileAdder.Progress = settings.Progress
	// }
	// fileAdder.Pin = settings.Pin && !settings.OnlyHash
	// fileAdder.Silent = settings.Silent
	// fileAdder.RawLeaves = settings.RawLeaves
	// fileAdder.NoCopy = settings.NoCopy
	// fileAdder.CidBuilder = prefix

	// switch settings.Layout {
	// case options.BalancedLayout:
	// 	// Default
	// case options.TrickleLayout:
	// 	fileAdder.Trickle = true
	// default:
	// 	return nil, fmt.Errorf("unknown layout: %d", settings.Layout)
	// }

	nd, err := fileAdder.AddFileAndPin(f)
	if err != nil {
		return cid.Undef, err
	}

	return nd.Cid(), nil
}

// syncDagService is used by the Adder to ensure blocks get persisted to the underlying datastore
type syncDagService struct {
	ipld.DAGService
	syncFn func() error
}

func (s *syncDagService) Sync() error {
	return s.syncFn()
}
