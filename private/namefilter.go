package private

import (
	"encoding/hex"
	"strings"

	"github.com/qri-io/wnfs-go/bloom"
	"golang.org/x/crypto/sha3"
)

type (
	// a name filter with just inumbers in it
	BareNamefilter string
	// a name filter with inumbers and a key, which serves as the revision
	// identifier
	KeyedNameFilter string
	// a hashed name filter exported as "private.Name", private.Name is in fact
	// a private value, and needs to be kept secret
	Name string
)

// parent is the "super" constructor arg
func NewBareNamefilter(parent BareNamefilter, in INumber) (bnf BareNamefilter, err error) {
	f, err := bloom.DecodeBase64(string(parent))
	if err != nil {
		return bnf, err
	}
	f.Add(in[:])
	return BareNamefilter(f.EncodeBase64()), nil
}

// create bare name filter with a single key
func CreateBare(key Key) (BareNamefilter, error) {
	empty := strings.Repeat("0", 32)
	return AddToBare(BareNamefilter(empty), key[:])
}

// add some string to a name filter
func AddToBare(bareFilter BareNamefilter, toAdd []byte) (BareNamefilter, error) {
	f, err := bloom.DecodeBase64(string(bareFilter))
	if err != nil {
		return "", err
	}
	hash := sha256String(toAdd)
	f.Add([]byte(hash))
	return BareNamefilter(f.EncodeBase64()), nil
}

// add the revision number to the name filter, salted with the AES key for the
// node
func AddKey(bareFilter BareNamefilter, key Key) (knf KeyedNameFilter, err error) {
	f, err := bloom.DecodeBase64(string(bareFilter))
	if err != nil {
		return knf, err
	}
	f.Add(key[:])
	return KeyedNameFilter(f.EncodeBase64()), nil
}

// saturate the filter to 320 bits and hash it with sha256 to give the private
// name that a node will be stored in the MMPT with
func ToName(knf KeyedNameFilter) (Name, error) {
	f, err := bloom.DecodeBase64(string(knf))
	if err != nil {
		return "", err
	}

	if err := f.Saturate(); err != nil {
		return "", err
	}
	return hashName(f), nil
}

func sha256String(v []byte) string {
	sum := sha3.Sum256(v)
	return hex.EncodeToString(sum[:])
}

// hash a filter with sha256
func hashName(f *bloom.Filter) Name {
	return Name(sha256String(f[:]))
}

// root key should be
// the identity bare name filter
func IdentityBareNamefilter() BareNamefilter {
	sum := sha3.Sum256(make([]byte, 32))
	f := &bloom.Filter{}
	f.Add(sum[:])
	return BareNamefilter(f.EncodeBase64())
}
