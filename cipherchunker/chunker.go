package cipherchunker

import (
	"crypto/cipher"
	"crypto/rand"
	"io"

	chunker "github.com/ipfs/go-ipfs-chunker"
	pool "github.com/libp2p/go-buffer-pool"
)

type cipherSplitter struct {
	cipher        cipher.AEAD
	r             io.Reader
	size          uint32
	plaintextSize int
	nonceSize     int
	err           error
}

var _ chunker.Splitter = (*cipherSplitter)(nil)

// NewSizeSplitter returns a new size-based Splitter with the given block size.
func NewCipherSplitter(r io.Reader, auth cipher.AEAD, size uint32) (chunker.Splitter, error) {
	return &cipherSplitter{
		cipher:        auth,
		r:             r,
		size:          size + uint32(auth.NonceSize()),
		plaintextSize: int(size) - auth.Overhead(),
		nonceSize:     auth.NonceSize(),
	}, nil
}

// NextBytes produces a new chunk.
func (cs *cipherSplitter) NextBytes() ([]byte, error) {
	if cs.err != nil {
		return nil, cs.err
	}

	plaintext := pool.Get(cs.plaintextSize)
	n, err := io.ReadFull(cs.r, plaintext)
	switch err {
	case io.ErrUnexpectedEOF:
		cs.err = io.EOF
		small := make([]byte, n)
		copy(small, plaintext)
		pool.Put(plaintext)
		return cs.encryptBlock(small)
	case nil:
		defer pool.Put(plaintext)
		return cs.encryptBlock(plaintext)
	default:
		pool.Put(plaintext)
		return nil, err
	}
}

func (cs *cipherSplitter) encryptBlock(plaintext []byte) ([]byte, error) {
	ciphertext := pool.Get(int(cs.size))
	if _, err := rand.Read(ciphertext[:cs.nonceSize]); err != nil {
		pool.Put(ciphertext)
		return nil, err
	}

	encrypted := cs.cipher.Seal(ciphertext[cs.nonceSize:cs.nonceSize], ciphertext[:cs.nonceSize], plaintext, nil)
	return ciphertext[:cs.nonceSize+len(encrypted)], nil
}

// Reader returns the io.Reader associated to this Splitter.
func (cs *cipherSplitter) Reader() io.Reader { return cs.r }
