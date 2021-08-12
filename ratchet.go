package wnfs

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

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

type SpiralRatchet struct {
	large      [32]byte
	medium     [32]byte // bounded to 256 elements
	mediumCeil [32]byte
	small      [32]byte // bounded to 256 elements
	smallCeil  [32]byte
}

func NewSpiralRatchet() *SpiralRatchet {
	seedData := make([]byte, 32, 32)
	if _, err := rand.Read(seedData); err != nil {
		panic(err)
	}
	seed := [32]byte{}
	for i, d := range seedData {
		seed[i] = d
	}
	return NewSpiralRatchetFromSeed(seed)
}

func NewSpiralRatchetFromSeed(seed [32]byte) *SpiralRatchet {
	medium := sha256.Sum256(binaryCompliment(seed))
	small := sha256.Sum256(binaryCompliment(medium))

	// TODO (use crypto random to scramble small & medium)

	return &SpiralRatchet{
		large:      sha256.Sum256(seed[:]),
		medium:     sha256.Sum256(medium[:]),
		mediumCeil: mediumCeil(medium),
		small:      sha256.Sum256(small[:]),
		smallCeil:  smallCeil(small),
	}
}

func DecodeRatchet(s string) (*SpiralRatchet, error) {
	data, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(data) != 5*32 {
		return nil, fmt.Errorf("invalid ratchet length")
	}

	fields := [5][32]byte{}
	for i, b := range data {
		fields[i/32][i%32] = b
	}

	return &SpiralRatchet{
		large:      fields[0],
		medium:     fields[1],
		mediumCeil: fields[2],
		small:      fields[3],
		smallCeil:  fields[4],
	}, nil
}

func (r *SpiralRatchet) Encode() string {
	b := &bytes.Buffer{}
	b.Write(r.large[:])
	b.Write(r.medium[:])
	b.Write(r.mediumCeil[:])
	b.Write(r.small[:])
	b.Write(r.smallCeil[:])
	return base64.URLEncoding.EncodeToString(b.Bytes())
}

func (r *SpiralRatchet) Key() Key {
	// xor is associative, so order shouldn't matter
	v := binaryXOR(binaryXOR(r.large, r.medium), r.small)
	return sha256.Sum256(v[:])
}

// Advance increments the ratchet
func (r *SpiralRatchet) Advance() {
	nextSmall := sha256.Sum256(r.small[:]) // increment small
	if nextSmall != r.smallCeil {
		// advance small
		r.small = nextSmall
		return
	}

	// small == smallCeil
	r.AdvanceMedium()
}

// JumpMedium fast-forwards the ratchet through 256 advances (2^8)
func (r *SpiralRatchet) AdvanceMedium() {
	nextMedium := sha256.Sum256(r.medium[:]) // increment medium
	// MUST reset small here.
	small := sha256.Sum256(binaryCompliment(nextMedium)) // reset small

	if nextMedium != r.mediumCeil {
		// advance medium

		*r = SpiralRatchet{
			large:      r.large,
			medium:     nextMedium,
			mediumCeil: r.mediumCeil,
			small:      small,
			smallCeil:  smallCeil(small),
		}
		return
	}

	r.AdvanceLarge()
}

func (r *SpiralRatchet) AdvanceLarge() {
	// advance large
	nextLarge := sha256.Sum256(r.large[:])
	*r = *NewSpiralRatchetFromSeed(nextLarge)
}

func smallCeil(d [32]byte) [32]byte {
	// ceiling is the first shasum after threshold (257th iteration)
	for i := 0; i < 256; i++ {
		d = sha256.Sum256(d[:])
	}
	return d
}

func mediumCeil(d [32]byte) [32]byte {
	// ceiling is the first shasum after threshold (257th iteration)
	for i := 0; i < 257; i++ {
		d = sha256.Sum256(d[:])
	}
	return d
}

func binaryXOR(a, b [32]byte) [32]byte {
	res := [32]byte{}
	for i := 0; i < 32; i++ {
		res[i] = a[i] ^ b[i]
	}
	return res
}

func binaryCompliment(d [32]byte) []byte {
	res := make([]byte, 32)
	for i, b := range d {
		res[i] = ^b
	}
	return res
}
