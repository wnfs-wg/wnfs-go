package cipherfile

import (
	"context"
	"crypto/cipher"
	"fmt"

	files "github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	unixfs "github.com/ipfs/go-unixfs"
)

type cipherFile struct {
	cipher cipher.AEAD
	DagReader
}

var _ files.File = (*cipherFile)(nil)

func NewCipherFile(ctx context.Context, dserv ipld.DAGService, nd ipld.Node, auth cipher.AEAD) (files.Node, error) {
	switch dn := nd.(type) {
	case *dag.ProtoNode:
		fsn, err := unixfs.FSNodeFromBytes(dn.Data())
		if err != nil {
			return nil, err
		}
		if fsn.IsDir() {
			return nil, fmt.Errorf("cipherfile does not support directories")
			// return newUnixfsDir(ctx, dserv, dn)
		}
		// if fsn.Type() == ft.TSymlink {
		// 	return files.NewLinkFile(string(fsn.Data()), nil), nil
		// }

	case *dag.RawNode:
	default:
		return nil, fmt.Errorf("unknown node type: %T", nd)
	}

	dr, err := NewDagReader(ctx, nd, dserv, auth)
	if err != nil {
		return nil, err
	}

	return &cipherFile{
		cipher:    auth,
		DagReader: dr,
	}, nil
}

func (f *cipherFile) Size() (int64, error) {
	return int64(f.DagReader.Size()), nil
}

func (f *cipherFile) Close() error {
	return nil
}
