package ratchet

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	golog "github.com/ipfs/go-log"
	sha3 "golang.org/x/crypto/sha3"
)

var log = golog.Logger("wnfs")

// Flag the encoding. The default encoding is:
// * base64URL-unpadded (signified with u)
// * SHA-256 (0x16: "F" in base64URL)
const ratchetSignifier = "uF"

type Spiral struct {
	large         [32]byte
	medium        [32]byte // bounded to 256 elements
	mediumCounter uint8
	small         [32]byte // bounded to 256 elements
	smallCounter  uint8
}

func NewSpiral() *Spiral {
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

	return &Spiral{
		large:  hash32(seed),
		medium: hash32N(medSeed, incMed),
		small:  hash32N(smallSeed, incSmall),
	}
}

func zero(seed [32]byte) Spiral {
	med := hash32(compliement(seed))
	small := hash32(compliement(med))
	return Spiral{
		large:  hash32(seed),
		medium: med,
		small:  small,
	}
}

func (r Spiral) Key() [32]byte {
	// xor is associative, so order shouldn't matter
	v := xor(r.large, xor(r.medium, r.small))
	return hash(v[:])
}

func (r *Spiral) Copy() *Spiral {
	return &Spiral{
		large:         r.large,
		medium:        r.medium,
		mediumCounter: r.mediumCounter,
		small:         r.small,
		smallCounter:  r.smallCounter,
	}
}

func DecodeSpiral(s string) (*Spiral, error) {
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

	r := &Spiral{}
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

func (r Spiral) Encode() string {
	b := &bytes.Buffer{}
	b.Write(r.small[:])
	b.WriteByte(r.smallCounter)
	b.Write(r.medium[:])
	b.WriteByte(r.mediumCounter)
	b.Write(r.large[:])
	return ratchetSignifier + base64.RawURLEncoding.EncodeToString(b.Bytes())
}

func (r *Spiral) Inc() {
	if r.smallCounter >= 255 {
		*r, _ = nextMediumEpoch(*r)
		return
	}

	r.small = hash32(r.small)
	r.smallCounter += 1
}

func (r *Spiral) IncBy(n int) {
	jumped, _ := incBy(*r, n)
	*r = jumped
}

var ErrUnknownRatchetRelation = fmt.Errorf("unknown")

func (r Spiral) Compare(b Spiral, maxSteps int) (int, error) {
	var (
		leftCounter  = r.combinedCounter()
		rightCounter = b.combinedCounter()
	)

	if r.large == b.large {
		if leftCounter == rightCounter {
			return 0, nil
		}
		return leftCounter - rightCounter, nil
	}

	// here, the large digit always differs. So one of the ratchets will always be bigger,
	// they can't be equal.
	// We can find out which one is bigger by hashing both at the same time and looking at
	// when one created the same digit as the other, essentially racing the large digit's
	// recursive hashes.

	var (
		leftLargeInital   = r.large
		rightLargeInitial = b.large
		leftLarge         = r.large
		rightLarge        = b.large
		leftLargeCounter  = 0
		rightLargeCounter = 0
	)

	// Since the two ratchets might just be generated from a totally different setup, we
	// can never _really_ know which one is the bigger one. They might be unrelated.

	for maxSteps > 0 {
		leftLarge = hash32(leftLarge)
		rightLarge = hash32(rightLarge)
		leftLargeCounter++
		rightLargeCounter++

		// largerCountAhead is how many `inc`s the larger one is head of the smaller one
		if rightLarge == leftLargeInital {
			// rightLargeCounter * 256*256 is the count of `inc`s applied via advancing the large digit continually
			// -rightCounter is the difference between `right` and its next large epoch.
			// leftCounter is just what's left to add because of the count at which `left` is.
			largerCountAhead := rightLargeCounter*256*256 - rightCounter + leftCounter
			return largerCountAhead, nil
		}

		if leftLarge == rightLargeInitial {
			// In this case, we compute the same difference, but return the negative to indicate
			// that `right` is bigger than `left` rather than the other way around.
			largerCountAhead := leftLargeCounter*256*256 - leftCounter + rightCounter
			return -largerCountAhead, nil
		}
		maxSteps--
	}

	return 0, ErrUnknownRatchetRelation
}

func (r Spiral) Equal(b Spiral) bool {
	return r.small == b.small &&
		r.smallCounter == b.smallCounter &&
		r.medium == b.medium &&
		r.mediumCounter == b.mediumCounter &&
		r.large == b.large
}

func (r Spiral) combinedCounter() int {
	return 256*int(r.mediumCounter) + int(r.smallCounter)
}

func incBy(r Spiral, n int) (jumped Spiral, jumpCount int) {
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

func nextLargeEpoch(r Spiral) (jumped Spiral, jumpCount int) {
	jumped = zero(r.large)
	jumpCount = 256*256 - r.combinedCounter()
	return jumped, jumpCount
}

func nextMediumEpoch(r Spiral) (jumped Spiral, jumpCount int) {
	if r.mediumCounter >= 255 {
		return nextLargeEpoch(r)
	}

	jumped = Spiral{
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
