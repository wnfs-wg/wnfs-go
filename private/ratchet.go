package private

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"golang.org/x/crypto/sha3"
)

// Flag the encoding. The default encoding is:
// * base64URL-unpadded (signified with u)
// * SHA-256 (0x16: "F" in base64URL)
const ratchetSignifier = "uF"

type SpiralRatchet struct {
	large         [32]byte
	medium        [32]byte // bounded to 256 elements
	mediumCounter uint8
	small         [32]byte // bounded to 256 elements
	smallCounter  uint8
}

func NewSpiralRatchet() *SpiralRatchet {
	seedData := make([]byte, 34) // 32 bytes for the seed, plus 2 extra bytes to randomize small & medium starts
	if _, err := rand.Read(seedData); err != nil {
		panic(err)
	}
	seed := [32]byte{}
	for i, d := range seedData[:32] {
		seed[i] = d
	}
	incMed, incSmall := seedData[32], seedData[33]
	medSeed := hash32(compliement(seed))
	smallSeed := hash32(compliement(medSeed))

	return &SpiralRatchet{
		large:  hash32(seed),
		medium: hash32N(medSeed, incMed),
		small:  hash32N(smallSeed, incSmall),
	}
}

func zero(seed [32]byte) SpiralRatchet {
	med := hash32(compliement(seed))
	small := hash32(compliement(med))
	return SpiralRatchet{
		large:  hash32(seed),
		medium: med,
		small:  small,
	}
}

func (r *SpiralRatchet) Key() Key {
	// xor is associative, so order shouldn't matter
	v := xor(r.large, xor(r.medium, r.small))
	return hash(v[:])
}

func (r *SpiralRatchet) Copy() *SpiralRatchet {
	return &SpiralRatchet{
		large:         r.large,
		medium:        r.medium,
		mediumCounter: r.mediumCounter,
		small:         r.small,
		smallCounter:  r.smallCounter,
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
			r.smallCounter = d
		case i >= 33 && i < 65:
			r.medium[i-33] = d
		case i == 65:
			r.mediumCounter = d
		case i >= 66 && i < 98:
			r.large[i-66] = d
		}
	}

	return r, nil
}

func (r *SpiralRatchet) Encode() string {
	b := &bytes.Buffer{}
	b.Write(r.small[:])
	b.WriteByte(r.smallCounter)
	b.Write(r.medium[:])
	b.WriteByte(r.mediumCounter)
	b.Write(r.large[:])
	return ratchetSignifier + base64.RawURLEncoding.EncodeToString(b.Bytes())
}

func (r *SpiralRatchet) Inc() {
	if r.smallCounter >= 255 {
		*r, _ = nextMediumEpoch(*r)
		return
	}

	r.small = hash32(r.small)
	r.smallCounter += 1
}

func (r *SpiralRatchet) IncBy(n int) {
	jumped, _ := incBy(*r, n)
	*r = jumped
}

func (r SpiralRatchet) combinedCounter() int {
	return 256*int(r.mediumCounter) + int(r.smallCounter)
}

func incBy(r SpiralRatchet, n int) (jumped SpiralRatchet, jumpCount int) {
	if n <= 0 {
		return r, 0
	} else if n >= 256*256-r.combinedCounter() {
		// n is larger than at least one large epoch jump
		jumped, jumpCount := nextLargeEpoch(r)
		return incBy(jumped, n-jumpCount)
	} else if n >= 256-int(r.smallCounter) {
		// n is larger than at least one medium epoch jump
		jumped, jumpCount := nextMediumEpoch(r)
		return incBy(jumped, n-jumpCount)
	}

	// increment by small. checks above ensure n < 256
	r.small = hash32N(r.small, uint8(n))
	r.smallCounter += uint8(n)
	return r, n
}

func nextLargeEpoch(r SpiralRatchet) (jumped SpiralRatchet, jumpCount int) {
	jumped = zero(r.large)
	jumpCount = 256*256 - r.combinedCounter()
	return jumped, jumpCount
}

func nextMediumEpoch(r SpiralRatchet) (jumped SpiralRatchet, jumpCount int) {
	if r.mediumCounter >= 255 {
		return nextLargeEpoch(r)
	}

	jumped = SpiralRatchet{
		large:         r.large,
		medium:        hash32(r.medium),
		mediumCounter: r.mediumCounter + 1,
		small:         hash32(compliement(r.medium)),
	}
	jumpCount = jumped.combinedCounter() - r.combinedCounter()
	return jumped, jumpCount
}

func hash(d []byte) [32]byte {
	return sha3.Sum256(d)
}

func hash32(d [32]byte) [32]byte {
	return hash(d[:])
}

func hash32N(d [32]byte, n uint8) [32]byte {
	for i := uint8(0); i < n; i++ {
		d = hash32(d)
	}
	return d
}

func xor(a, b [32]byte) [32]byte {
	res := [32]byte{}
	for i := 0; i < 32; i++ {
		res[i] = a[i] ^ b[i]
	}
	return res
}

func compliement(d [32]byte) [32]byte {
	res := [32]byte{}
	for i, b := range d {
		res[i] = ^b
	}
	return res
}
