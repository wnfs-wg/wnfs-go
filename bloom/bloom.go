package bloom

import (
	"encoding/base64"
	"fmt"

	"github.com/pierrec/xxHash/xxHash32"
)

// this bloom filter is hardcoded with an m of 2048 bits (256 bytes) and 30
// hashes
const (
	M = 256 // 2048 bits / 8 bits per byte = 256 bytes
	K = 30
)

type Filter [M]byte

func FromBase64(s string) (*Filter, error) {
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

func (f Filter) ToBase64() string {
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
