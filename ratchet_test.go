package wnfs

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestRatchet(t *testing.T) {
	// seed pulled from https://whitepaper.fission.codes/file-system/partitions/private-directories/concepts/spiral-ratchet
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")

	a := NewSpiralRatchetFromSeed(seed)
	assertRatchet(t, a, map[string]string{
		"large":      "8e2023cc8b9b279c5f6eb03938abf935dde93be9bfdc006a0f570535fda82ef8",
		"medium":     "99009e29ac6546ebce395d4b9b3a4c0eeafa8b3a814c4343a1a7a47893655e69",
		"mediumCeil": "046d10975da721f0e92a2329ef12e6e53617a23babf652b16653eb0597750758",
		"small":      "c18a8da7f51d2327b9712f63c357bd6316df737df0493a4b18ec691dece559c9",
		"smallCeil":  "23605df95870dec1a2de51e3de80dd082d8dded3b8041f34667e1cce8a48e3d1",
	})

	// prove a single advance is sane
	a.Advance()
	b := NewSpiralRatchetFromSeed(seed)
	b.Advance()
	assertRatchetsEqual(t, a, b)

	aH := a.Key()
	bH := b.Key()
	if aH != bH {
		t.Errorf("version hash mismatch %x != %x", aH, bH)
	}
}

func TestRatchetAdvanceMedium(t *testing.T) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	// manually advance ratchet 256 (2 ^ 8) times
	slow := NewSpiralRatchetFromSeed(seed)
	for i := 0; i < 255; i++ {
		slow.Advance()
	}

	// fast jump 256 values in one shot
	fast := NewSpiralRatchetFromSeed(seed)
	fast.AdvanceMedium()

	assertRatchetsEqual(t, slow, fast)
}

func TestRatchetAdvanceLarge(t *testing.T) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	// manually advance ratchet 2 ^ 16 times
	slow := NewSpiralRatchetFromSeed(seed)
	for i := 0; i < 65535; i++ {
		slow.Advance()
	}

	// fast jump 2 ^ 16 values in one shot
	fast := NewSpiralRatchetFromSeed(seed)
	fast.AdvanceLarge()

	assertRatchetsEqual(t, slow, fast)
}

func TestRatchetCoding(t *testing.T) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	a := NewSpiralRatchetFromSeed(seed)
	encoded := a.Encode()

	b, err := DecodeRatchet(encoded)
	if err != nil {
		t.Fatal(err)
	}

	assertRatchetsEqual(t, a, b)
}

func TestBinaryCompliment(t *testing.T) {
	zeros := [32]byte{}
	ones := bytes.Repeat([]byte{255}, 32)
	if !bytes.Equal(binaryCompliment(zeros), ones) {
		t.Errorf("expected compliment of empty bytes to be the ceil of a 32 byte slice")
	}
}

func TestBinaryXOR(t *testing.T) {
	zeros := [32]byte{}
	ones := [32]byte{
		255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255,
	}

	if binaryXOR(zeros, zeros) != zeros {
		t.Errorf("zeros^zeros must equal zeros")
	}
	if binaryXOR(zeros, ones) != ones {
		t.Errorf("zeros^ones must equal ones")
	}
}

func assertRatchetsEqual(t *testing.T, a, b *SpiralRatchet) {
	t.Helper()
	if diff := cmp.Diff(hexMap(a), hexMap(b)); diff != "" {
		t.Errorf("ratchet mismatch (-a +b):\n%s", diff)
	}
}

func assertRatchet(t *testing.T, r *SpiralRatchet, expect map[string]string) {
	t.Helper()
	got := hexMap(r)
	if diff := cmp.Diff(expect, got); diff != "" {
		t.Errorf("ratchet value mismatch (-want +got):\n%s", diff)
	}
}

func shasumFromHex(s string) [32]byte {
	d, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	res := [32]byte{}
	for i, b := range d {
		res[i] = b
	}
	return res
}

func hexMap(r *SpiralRatchet) map[string]string {
	return map[string]string{
		"large":      hex.EncodeToString(r.large[:]),
		"medium":     hex.EncodeToString(r.medium[:]),
		"mediumCeil": hex.EncodeToString(r.mediumCeil[:]),
		"small":      hex.EncodeToString(r.small[:]),
		"smallCeil":  hex.EncodeToString(r.smallCeil[:]),
	}
}
