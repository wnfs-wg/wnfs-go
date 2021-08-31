package private

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
		"large":        "8e2023cc8b9b279c5f6eb03938abf935dde93be9bfdc006a0f570535fda82ef8",
		"medium":       "99009e29ac6546ebce395d4b9b3a4c0eeafa8b3a814c4343a1a7a47893655e69",
		"mediumCursor": "00",
		"small":        "c18a8da7f51d2327b9712f63c357bd6316df737df0493a4b18ec691dece559c9",
		"smallCursor":  "00",
	})

	// prove a single advance is sane
	a.Add1()
	b := NewSpiralRatchetFromSeed(seed)
	b.Add1()
	assertRatchetsEqual(t, a, b)

	aH := a.Key()
	bH := b.Key()
	if aH != bH {
		t.Errorf("version hash mismatch %x != %x", aH, bH)
	}
}

func TestRatchetAdd256(t *testing.T) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	// manually advance ratchet 256 (2 ^ 8) times
	slow := NewSpiralRatchetFromSeed(seed)
	for i := 0; i < 256; i++ {
		slow.Add1()
	}

	// fast jump 256 values in one shot
	fast := NewSpiralRatchetFromSeed(seed)
	fast.Add256()

	assertRatchetsEqual(t, slow, fast)
}

func TestRatchetAdd1000(t *testing.T) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	// manually advance ratchet 2 ^ 16 times (65,536)
	slow := NewSpiralRatchetFromSeed(seed)
	for i := 0; i < 1000; i++ {
		slow.Add1()
	}

	// fast jump 2 ^ 16 values in one shot
	fast := NewSpiralRatchetFromSeed(seed)
	fast.Add256()
	fast.Add256()
	fast.Add256()
	for i := 0; i < 1000-768; i++ {
		fast.Add1()
	}

	assertRatchetsEqual(t, slow, fast)
}

func TestRatchetAdd65536(t *testing.T) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	// manually advance ratchet 2 ^ 16 times (65,536)
	slow := NewSpiralRatchetFromSeed(seed)
	for i := 0; i < 65536; i++ {
		slow.Add1()
	}

	// fast jump 2 ^ 16 values in one shot
	fast := NewSpiralRatchetFromSeed(seed)
	fast.Add65536()

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

func TestCompliment(t *testing.T) {
	zeros := [32]byte{}
	ones := bytes.Repeat([]byte{255}, 32)
	if !bytes.Equal(compliement(zeros), ones) {
		t.Errorf("expected compliment of empty bytes to be the ceil of a 32 byte slice")
	}
}

func TestXOR(t *testing.T) {
	zeros := [32]byte{}
	ones := [32]byte{
		255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255,
	}

	if xor(zeros, zeros) != zeros {
		t.Errorf("zeros^zeros must equal zeros")
	}
	if xor(zeros, ones) != ones {
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
		"large":        hex.EncodeToString(r.large[:]),
		"medium":       hex.EncodeToString(r.medium[:]),
		"mediumCursor": hex.EncodeToString([]byte{r.mediumCursor}),
		"small":        hex.EncodeToString(r.small[:]),
		"smallCursor":  hex.EncodeToString([]byte{r.smallCursor}),
	}
}

func BenchmarkRatchetAdd256(b *testing.B) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	for i := 0; i < b.N; i++ {
		r := NewSpiralRatchetFromSeed(seed)
		for i := 0; i < 255; i++ {
			r.Add1()
		}
	}
}

func BenchmarkRatchetDeserializeAdd1(b *testing.B) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	r := NewSpiralRatchetFromSeed(seed)
	// advance ratchet a bunch
	for i := 0; i < 125; i++ {
		r.Add1()
	}
	// serialize to a string
	enc := r.Encode()

	// confirm ratchet will decode
	if _, err := DecodeRatchet(enc); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r, _ = DecodeRatchet(enc)
		r.Add1()
	}
}
