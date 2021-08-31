package private

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"

	bloom "github.com/bits-and-blooms/bloom/v3"
)

const (
	filterSize          = 2048
	hashCount           = 30
	saturationThreshold = 320
)

type (
	// a name filter with just inumbers in it
	BareNamefilter string
	// a name filter with inumbers and a key, which serves as the revision
	// identifier
	KeyedNameFilter string
	// a hashed name filter
	PrivateName string
	// // a name filter with path elements & revision number in it, saturated
	// // to ~320 bits
	// SaturatedNamefilter string
)

// parent is the "super" constructor arg
func NewBareNamefilter(parent BareNamefilter, in INumber) (bnf BareNamefilter, err error) {
	f, err := FromBase64(string(parent))
	if err != nil {
		return bnf, err
	}
	f.Add(in[:])
	return BareNamefilter(ToBase64(f)), nil
}

// create bare name filter with a single key
func CreateBare(key Key) (BareNamefilter, error) {
	empty := strings.Repeat("0", 32)
	return AddToBare(BareNamefilter(empty), key[:])
}

// add some string to a name filter
func AddToBare(bareFilter BareNamefilter, toAdd []byte) (BareNamefilter, error) {
	f, err := FromBase64(string(bareFilter))
	if err != nil {
		return "", err
	}
	hash := sha256String(toAdd)
	f.Add([]byte(hash))
	return BareNamefilter(ToBase64(f)), nil
}

// add the revision number to the name filter, salted with the AES key for the
// node
func AddKey(bareFilter BareNamefilter, key Key) (knf KeyedNameFilter, err error) {
	f, err := FromBase64(string(bareFilter))
	if err != nil {
		return knf, err
	}
	f.Add(key[:])
	return KeyedNameFilter(ToBase64(f)), nil
	// bnf, err := AddToBare(bareFilter, []byte(fmt.Sprintf("%d%x", revision, key)))
	// return KeyedNameFilter(bnf), err
}

// saturate the filter to 320 bits and hash it with sha256 to give the private
// name that a node will be stored in the MMPT with
func ToPrivateName(knf KeyedNameFilter) (PrivateName, error) {
	f, err := FromBase64(string(knf))
	if err != nil {
		return "", err
	}

	if err := saturateFilter(f, saturationThreshold); err != nil {
		return "", err
	}
	return toHash(f), nil
}

// // Saturate a filter (string) to 320 bits
// func Saturate(rnf KeyedNameFilter) (SaturatedNamefilter, error) {
// 	f, err := FromBase64(string(rnf))
// 	if err != nil {
// 		return "", err
// 	}
// 	if err := saturateFilter(f, saturationThreshold); err != nil {
// 		return "", err
// 	}
// 	s := ToBase64(f)
// 	return SaturatedNamefilter(s), nil
// }

// saturate a filter to 320 bits
func saturateFilter(f *bloom.BloomFilter, threshold int) error {
	fBytes := toBytes(f)
	if threshold > len(fBytes)*8 {
		return errors.New("threshold is bigger than filter size")
	}
	bits := countOnes(f)
	if bits >= threshold {
		return nil
	}

	// add hash of filter to saturate
	// theres a chance that the hash will collide with the existing filter and this gets stuck in an infinite loop
	// in that case keep re-hashing the hash & adding to the filter until there is no collision
	before := fBytes
	toHash := before
	for bytes.Equal(before, toBytes(f)) {
		hash := sha256String(toHash)
		f.Add([]byte(hex.EncodeToString([]byte(hash))))
		toHash = []byte(hash)
	}

	return saturateFilter(f, threshold)
}

// count the number of 1 bits in a filter
func countOnes(f *bloom.BloomFilter) int {
	// TODO(b5): should be a uint32 array
	data := toBytes(f)
	count := 0
	for _, b := range data {
		count += bitCount32(uint32(b))
	}
	return count
}

func sha256String(v []byte) string {
	sum := sha256.Sum256(v)
	return hex.EncodeToString(sum[:])
}

// hash a filter with sha256
func toHash(f *bloom.BloomFilter) PrivateName {
	return PrivateName(sha256String(toBytes(f)))
}

func toBytes(f *bloom.BloomFilter) []byte {
	// TODO(b5): should be able to pre-allocate byte length
	buf := &bytes.Buffer{}
	if _, err := f.WriteTo(buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// convert a filter to hex
func ToBase64(f *bloom.BloomFilter) string {
	buf := &bytes.Buffer{}
	if _, err := f.WriteTo(buf); err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(buf.Bytes())
}

// convert hex to a BloomFilter object
func FromBase64(s string) (*bloom.BloomFilter, error) {
	buf, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	f := &bloom.BloomFilter{}
	if _, err := f.ReadFrom(bytes.NewBuffer(buf)); err != nil {
		return nil, err
	}
	return f, nil
}

// counts the number of 1s in a uint32
// from: https://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel
func bitCount32(num uint32) int {
	a := num - ((num >> 1) & 0x55555555)
	b := (a & 0x33333333) + ((a >> 2) & 0x33333333)
	return int(((b + (b>>4)&0xF0F0F0F) * 0x1010101) >> 24)
}

// root key should be
// the identity bare name filter
func IdentityBareNamefilter() BareNamefilter {
	sum := sha256.Sum256(make([]byte, 32))
	f := bloom.New(2048, 30)
	f.Add(sum[:])
	return BareNamefilter(ToBase64(f))
}
