package wnfs

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"sync"
)

// TODO(b5): make this equal the max size of one IPFS block
const MaxChunkSize = 65535

type Key [32]byte

func NewKey() Key {
	return NewSpiralRatchet().Key()
}

func (k Key) IsEmpty() bool { return k == Key([32]byte{}) }

func (k Key) MarshalJSON() ([]byte, error) {
	return []byte(`"` + base64.URLEncoding.EncodeToString(k[:]) + `"`), nil
}

func (k *Key) UnmarshalJSON(d []byte) error {
	var s string
	if err := json.Unmarshal(d, &s); err != nil {
		return err
	}
	data, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	for i, d := range data {
		k[i] = d
	}
	return nil
}

// func newCipher(key Key) (cipher.AEAD, error) {
// 	block, err := aes.NewCipher(key[:])
// 	if err != nil {
// 		return nil, err
// 	}
// 	return cipher.NewGCM(block)
// }

// func encrypt(key Key, data []byte) ([]byte, error) {
// 	gcm, err := newCipher(key)
// 	if err != nil {
// 		return nil, err
// 	}

// 	nonce := make([]byte, gcm.NonceSize())
// 	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
// 		return nil, err
// 	}

// 	ciphertext := gcm.Seal(nonce, nonce, data, nil)
// 	return ciphertext, nil
// }

// func decrypt(enc io.ReadCloser, key Key) ([]byte, error) {
// 	data, err := ioutil.ReadAll(enc)
// 	if err != nil {
// 		return nil, err
// 	}
// 	gcm, err := newCipher(key)
// 	if err != nil {
// 		return nil, err
// 	}

// 	nonceSize := gcm.NonceSize()
// 	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
// 	return gcm.Open(nil, nonce, ciphertext, nil)
// }

// Cipher provides encrypt and decrypt function of a slice data.
type Cipher interface {
	// Encrypt encrypts a plaintext to ciphertext. Returns ciphertext slice
	// whose length should be equal to ciphertext length. Input buffer ciphertext
	// has enough length for encrypted plaintext, and the length satisfy:
	// 	len(ciphertext) == MaxChunkSize + MaxOverheadSize
	// 	len(plaintext) <= MaxChunkSize
	Encrypt(ciphertext, plaintext []byte) ([]byte, error)

	// Decrypt decrypts a ciphertext to plaintext. Returns plaintext slice
	// whose length should be equal to plaintext length. Input buffer plaintext
	// has enough length for decrypted ciphertext, and the length satisfy:
	//	len(plaintext) == MaxChunkSize
	//	len(ciphertext) <= MaxChunkSize + MaxOverheadSize
	Decrypt(plaintext, ciphertext []byte) ([]byte, error)

	// MaxOverhead is the max number of bytes overhead of ciphertext compared to
	// plaintext. This should contain both encryption overhead and size of
	// additional data in ciphertext like nonce. It is only used to determine
	// internal buffer size, so overestimate is ok.
	MaxOverhead() int
}

// CryptoAEADCipher is a wrapper to crypto/cipher AEAD interface and implements
// Cipher interface.
type CryptoAEADCipher struct {
	aead cipher.AEAD
}

// NewCryptoAEADCipher converts a crypto/cipher AEAD to Cipher.
func NewCryptoAEADCipher(aead cipher.AEAD) *CryptoAEADCipher {
	return &CryptoAEADCipher{
		aead: aead,
	}
}

// Encrypt implements Cipher.
func (c *CryptoAEADCipher) Encrypt(ciphertext, plaintext []byte) ([]byte, error) {
	nonceSize := c.aead.NonceSize()
	if _, err := rand.Read(ciphertext[:nonceSize]); err != nil {
		return nil, err
	}

	encrypted := c.aead.Seal(ciphertext[nonceSize:nonceSize], ciphertext[:nonceSize], plaintext, nil)
	return ciphertext[:nonceSize+len(encrypted)], nil
}

// Decrypt implements Cipher.
func (c *CryptoAEADCipher) Decrypt(plaintext, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) <= c.overhead() {
		return nil, fmt.Errorf("invalid ciphertext size %d", len(ciphertext))
	}

	plaintext, err := c.aead.Open(plaintext[:0], ciphertext[:c.aead.NonceSize()], ciphertext[c.aead.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt failed: %v", err)
	}

	return plaintext, nil
}

// overhead returns Cipher's overhead including nonce size.
func (c *CryptoAEADCipher) overhead() int {
	return c.aead.NonceSize() + c.aead.Overhead()
}

// MaxOverhead implements Cipher.
func (c *CryptoAEADCipher) MaxOverhead() int {
	return c.overhead()
}

// NewAESGCMCipher creates a 128-bit (16 bytes key) or 256-bit (32 bytes key)
// AES block cipher wrapped in Galois Counter Mode with the standard nonce
// length.
func NewAESGCMCipher(key Key) (*CryptoAEADCipher, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return NewCryptoAEADCipher(aesgcm), nil
}

type encryptingReader struct {
	cipher Cipher
	fi     fsFileInfo

	r      io.Reader
	lock   sync.RWMutex
	closed bool
	done   bool

	buf              *bytes.Buffer
	encryptBuffer    []byte
	encryptLenBuffer []byte
}

var _ fs.File = (*encryptingReader)(nil)

func newEncryptingFile(r io.Reader, key Key, fi fsFileInfo) (f fs.File, err error) {
	// TODO(b5): make cipher pluggable
	cipher, err := NewAESGCMCipher(key)
	if err != nil {
		return nil, err
	}

	return &encryptingReader{
		cipher:           cipher,
		fi:               fi,
		r:                r,
		buf:              bytes.NewBuffer(make([]byte, 0, MaxChunkSize)),
		encryptBuffer:    make([]byte, MaxChunkSize+cipher.MaxOverhead()),
		encryptLenBuffer: make([]byte, 4),
	}, nil
}

func (er *encryptingReader) isClosed() bool {
	er.lock.RLock()
	defer er.lock.RUnlock()
	return er.closed
}

func (er *encryptingReader) Stat() (fs.FileInfo, error) {
	return er.fi, nil
}

// calls to read produce a stream of encrypted bytes
func (er *encryptingReader) Read(p []byte) (int, error) {
	for len(p) > er.buf.Len() && !er.done {
		// read up to MaxBlockSize of plaintext
		pt := make([]byte, MaxChunkSize)
		ptNum, err := er.r.Read(pt)
		if errors.Is(err, io.EOF) {
			er.done = true
			err = nil
		} else if err != nil {
			return 0, err
		}
		// trim buffer if it's smaller than a block
		if ptNum < MaxChunkSize {
			pt = pt[:ptNum]
			er.done = true
		}

		// encrypt plaintext, adding a nonce to the front
		ct := make([]byte, MaxChunkSize+er.cipher.MaxOverhead())
		ct, err = er.cipher.Encrypt(ct, pt)
		if err != nil {
			return 0, err
		}

		lenbuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenbuf, uint32(len(ct)))

		er.buf.Write(lenbuf)
		er.buf.Write(ct)
	}

	return er.buf.Read(p)
}

func writeVarBytes(writer io.Writer, b, lenBuf []byte) error {
	log.Debugw("writing bytes", "len", len(b))
	if len(b) > math.MaxInt32 {
		return errors.New("data size too large")
	}

	if len(lenBuf) < 4 {
		lenBuf = make([]byte, 4)
	}

	binary.LittleEndian.PutUint32(lenBuf, uint32(len(b)))

	_, err := writer.Write(lenBuf)
	if err != nil {
		return err
	}

	_, err = writer.Write(b)
	if err != nil {
		return err
	}

	return nil
}

func (er *encryptingReader) Close() error {
	er.lock.Lock()
	defer er.lock.Unlock()
	if er.closed {
		return nil
	}

	if closer, ok := er.r.(io.Closer); ok {
		return closer.Close()
	}
	er.closed = true
	return nil
}

type decryptingReader struct {
	cipher Cipher
	r      io.Reader
	lock   sync.RWMutex
	closed bool

	readLenBuffer   []byte
	readBuffer      []byte
	decryptBuffer   []byte
	decryptBufStart int
	decryptBufEnd   int
}

func newDecryptingReader(r io.Reader, key Key) (io.ReadCloser, error) {
	// TODO(b5): make cipher pluggable
	cipher, err := NewAESGCMCipher(key)
	if err != nil {
		return nil, err
	}

	return &decryptingReader{
		r:             r,
		cipher:        cipher,
		readBuffer:    make([]byte, MaxChunkSize+cipher.MaxOverhead()),
		decryptBuffer: make([]byte, MaxChunkSize),
		readLenBuffer: make([]byte, 4),
	}, nil
}

// isClosed returns whether the EncryptedStream is closed.
func (dr *decryptingReader) isClosed() bool {
	dr.lock.RLock()
	defer dr.lock.RUnlock()
	return dr.closed
}

// Read implements net.Conn and io.Reader
func (dr *decryptingReader) Read(b []byte) (int, error) {
	if dr.isClosed() {
		return 0, io.ErrClosedPipe
	}

	dr.lock.Lock()
	defer dr.lock.Unlock()

	if dr.decryptBufStart >= dr.decryptBufEnd {
		n, err := readVarBytes(dr.r, dr.readBuffer, dr.readLenBuffer)
		if err != nil {
			return 0, err
		}

		if n > MaxChunkSize+dr.cipher.MaxOverhead() {
			return 0, fmt.Errorf("received invalid encrypted data size %d", n)
		}

		dr.decryptBuffer = dr.decryptBuffer[:cap(dr.decryptBuffer)]
		dr.decryptBuffer, err = dr.cipher.Decrypt(dr.decryptBuffer, dr.readBuffer[:n])
		if err != nil {
			return 0, err
		}

		dr.decryptBufStart = 0
		dr.decryptBufEnd = len(dr.decryptBuffer)
	}

	n := copy(b, dr.decryptBuffer[dr.decryptBufStart:dr.decryptBufEnd])
	dr.decryptBufStart += n

	return n, nil
}

func (dr *decryptingReader) Close() error {
	dr.lock.Lock()
	defer dr.lock.Unlock()
	if dr.closed {
		return nil
	}

	if closer, ok := dr.r.(io.Closer); ok {
		return closer.Close()
	}
	dr.closed = true
	return nil
}

func readVarBytes(reader io.Reader, b, lenBuf []byte) (int, error) {
	if len(lenBuf) < 4 {
		lenBuf = make([]byte, 4)
	}
	_, err := io.ReadFull(reader, lenBuf)
	if err != nil {
		return 0, err
	}

	n := int(binary.LittleEndian.Uint32(lenBuf))
	if len(b) < n {
		return 0, io.ErrShortBuffer
	}

	return io.ReadFull(reader, b[:n])
}
