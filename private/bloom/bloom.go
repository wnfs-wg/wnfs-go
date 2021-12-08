package bloom

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/pierrec/xxHash/xxHash32"
	"golang.org/x/crypto/sha3"
)

// this bloom filter is hardcoded with an m of 2048 bits (256 bytes) and 30
// hashes
const (
	M                   = 256  // 2048 bits / 8 bits per byte = 256 bytes
	K                   = 30   // wnfs bloom filters use 30 hashes
	SaturationThreshold = 1019 // ones-count obfuscation minimum
)

type Filter [M]byte

func DecodeBase64(s string) (*Filter, error) {
	d, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(d) != M {
		return nil, fmt.Errorf("invalid Filter length")
	}
	f := &Filter{}
	for i, b := range d {
		f[i] = b
	}
	return f, nil
}

func (f Filter) EncodeBase64() string {
	return base64.URLEncoding.EncodeToString(f[:])
}

func (f Filter) Has(element []byte) bool {
	for index := range indiciesFor(element) {
		if !f.getBit(index) {
			return false
		}
	}
	return true
}

func (f *Filter) Add(element []byte) {
	for index := range indiciesFor(element) {
		f.setBit(index)
	}
}

func (f Filter) Copy() *Filter {
	cp := &Filter{}
	for i, d := range f {
		cp[i] = d
	}
	return cp
}

func (f Filter) Equals(b Filter) bool {
	for i, a := range f {
		if a != b[i] {
			return false
		}
	}
	return true
}

func (f *Filter) Saturate() error {
	bits := countOnes(f[:])
	if bits >= SaturationThreshold {
		return nil
	}

	// add hash of filter to saturate
	// theres a chance that the hash will collide with the existing filter,
	// sticking the recusrion function this gets stuck in an infinite loop
	// in that case keep re-hashing the hash & adding to the filter until
	// there is no collision
	before := f.Copy()

	for f.Equals(*before) {
		hash := sha256String(f[:])
		f.Add([]byte(hash))
	}

	return f.Saturate()
}

func (f *Filter) setBit(bitIndex uint32) {
	byteIndex := (bitIndex / 8) | 0
	indexWithinByte := bitIndex % 8
	f[byteIndex] = f[byteIndex] | (1 << indexWithinByte)
}

func (f Filter) getBit(bitIndex uint32) bool {
	byteIndex := (bitIndex / 8) | 0
	indexWithinByte := bitIndex % 8
	return (f[byteIndex] & (1 << indexWithinByte)) != 0
}

func indiciesFor(element []byte) <-chan uint32 {
	const m = M * 8
	const uint32Limit = 0x1_0000_0000
	x := xxHash32.Checksum(element, 0)
	y := xxHash32.Checksum(element, 1)
	res := make(chan uint32)
	go func() {
		res <- x % m
		for i := uint32(1); i < K; i++ {
			x = uint32(int(x+y) % uint32Limit)
			y = uint32(int(y+i) % uint32Limit)
			res <- x % m
		}
		close(res)
	}()
	return res
}

// count the number of 1 bits in a filter
func countOnes(data []byte) int {
	count := 0
	for _, b := range data {
		count += bitCount32(uint32(b))
	}
	return count
}

// counts the number of 1s in a uint32
// from: https://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel
func bitCount32(num uint32) int {
	a := num - ((num >> 1) & 0x55555555)
	b := (a & 0x33333333) + ((a >> 2) & 0x33333333)
	return int(((b + (b>>4)&0xF0F0F0F) * 0x1010101) >> 24)
}

func sha256String(v []byte) string {
	sum := sha3.Sum256(v)
	return hex.EncodeToString(sum[:])
}
