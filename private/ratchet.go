package private

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// Flag the encoding. The default encoding is:
// * base64URL-unpadded (signified with u)
// * SHA-256 (0x16: "F" in base64URL)
const ratchetSignifier = "uF"

type SpiralRatchet struct {
	large        [32]byte
	medium       [32]byte // bounded to 256 elements
	mediumCursor byte     // used as a uint8
	small        [32]byte // bounded to 256 elements
	smallCursor  byte     // used as a uint8
}

func NewSpiralRatchet() *SpiralRatchet {
	seedData := make([]byte, 32)
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
	medium := sha256.Sum256(compliement(seed))
	small := sha256.Sum256(compliement(medium))

	// TODO (use crypto.Random to scramble small & medium)

	return &SpiralRatchet{
		large:  sha256.Sum256(seed[:]),
		medium: sha256.Sum256(medium[:]),
		small:  sha256.Sum256(small[:]),
	}
}

func DecodeRatchet(s string) (*SpiralRatchet, error) {
	if len(s) != 133 {
		return nil, fmt.Errorf("invalid ratchet length")
	}

	if s[:2] != ratchetSignifier {
		return nil, fmt.Errorf("unsupported ratched encoding: %q. only %q is supported", s[:2], ratchetSignifier)
	}
	data, err := base64.RawURLEncoding.DecodeString(s[2:])
	if err != nil {
		return nil, err
	}

	r := &SpiralRatchet{}
	for i, d := range data {
		switch {
		case i < 32:
			r.small[i] = d
		case i == 32:
			r.smallCursor = d
		case i >= 33 && i < 65:
			r.medium[i-33] = d
		case i == 65:
			r.mediumCursor = d
		case i >= 66 && i < 98:
			r.large[i-66] = d
		}
	}

	return r, nil
}

func (r *SpiralRatchet) Encode() string {
	b := &bytes.Buffer{}
	b.Write(r.small[:])
	b.WriteByte(r.smallCursor)
	b.Write(r.medium[:])
	b.WriteByte(r.mediumCursor)
	b.Write(r.large[:])
	return ratchetSignifier + base64.RawURLEncoding.EncodeToString(b.Bytes())
}

func (r *SpiralRatchet) Key() Key {
	// xor is associative, so order shouldn't matter
	v := xor(xor(r.large, r.medium), r.small)
	return sha256.Sum256(v[:])
}

func (r *SpiralRatchet) Add1() {
	if r.smallCursor >= 255 {
		r.Add256()
		return
	}
	r.small = sha256.Sum256(r.small[:])
	r.smallCursor++
}

func (r *SpiralRatchet) Add256() {
	if r.mediumCursor >= 255 {
		r.Add65536()
		return
	}
	r.rolloverSmall()
	r.medium = sha256.Sum256(r.medium[:])
	r.mediumCursor++
}

func (r *SpiralRatchet) Add65536() {
	r.rolloverMedium()
	r.large = sha256.Sum256(r.large[:])
}

func (r *SpiralRatchet) rolloverMedium() {
	// TODO(b5): FIXME! this is incorrect. need to work through large field incrementing
	r.medium = sha256.Sum256(compliement(r.large))
	r.mediumCursor = 0
	r.rolloverSmall()
}

func (r *SpiralRatchet) rolloverSmall() {
	r.small = sha256.Sum256(compliement(r.medium))
	r.smallCursor = 0
}

func xor(a, b [32]byte) [32]byte {
	res := [32]byte{}
	for i := 0; i < 32; i++ {
		res[i] = a[i] ^ b[i]
	}
	return res
}

func compliement(d [32]byte) []byte {
	res := make([]byte, 32)
	for i, b := range d {
		res[i] = ^b
	}
	return res
}
