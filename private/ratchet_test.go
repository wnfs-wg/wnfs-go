package private

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRatchet(t *testing.T) {
	// seed pulled from https://whitepaper.fission.codes/file-system/partitions/private-directories/concepts/spiral-ratchet
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")

	a := zero(seed)
	assertRatchet(t, &a, map[string]string{
		"large":        "5aa00b14dd50887cdc0b0b55aa2da1eb5cc3a79cdbe893b2319da378a83ad0c5",
		"medium":       "5a86c2477e2ae4ffcf6373cce82259eb542b72a72db9cf9cddfe06bcc20623b6",
		"mediumCursor": "00",
		"small":        "962b7f9ac204ffd0fa398e9c875c90806c0cd6646655f7a5994b7a828b70c0da",
		"smallCursor":  "00",
	})

	// prove a single advance is sane
	a.Inc()
	b := zero(seed)
	b.Inc()
	assertRatchetsEqual(t, &a, &b)

	aH := a.Key()
	bH := b.Key()
	if aH != bH {
		t.Errorf("version hash mismatch %x != %x", aH, bH)
	}
}

func TestRatchetAdd256(t *testing.T) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	// manually advance ratchet 256 (2 ^ 8) times
	slow := zero(seed)
	for i := 0; i < 256; i++ {
		slow.Inc()
	}

	// fast jump 256 values in one shot
	fast := zero(seed)
	fast, _ = nextMediumEpoch(fast)

	assertRatchetsEqual(t, &slow, &fast)
}

func TestFuzzRatchet(t *testing.T) {
	// TODO(b5): flip this to a native fuzz test if/when native fuzzing lands
	// https://golang.org/s/draft-fuzzing-design
	f := fuzz.New()
	var n uint16 // test is relatively expensive, keeping the upper-bound limited keeps tests fast

	for count := 0; count < 50; count++ {
		f.Fuzz(&n) // get a random number to increment
		if n == 0 {
			continue // this test cannot test inc by zero
		}
		t.Logf("testing %d increments", n)
		slow := NewSpiralRatchet()
		fast := slow.Copy()

		for i := 0; i < int(n); i++ {
			slow.Inc()
		}

		fast.IncBy(int(n))
		assertRatchetsEqual(t, slow, fast)
	}
}
func TestRatchetAdd65536(t *testing.T) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	// manually advance ratchet 2 ^ 16 times (65,536)
	slow := zero(seed)
	for i := 0; i < 65536; i++ {
		slow.Inc()
	}

	// fast jump 2 ^ 16 values in one shot
	fast := zero(seed)
	fast, _ = nextLargeEpoch(fast)

	assertRatchetsEqual(t, &slow, &fast)
}

func TestRatchetCoding(t *testing.T) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	a := zero(seed)
	encoded := a.Encode()

	b, err := DecodeRatchet(encoded)
	if err != nil {
		t.Fatal(err)
	}

	assertRatchetsEqual(t, &a, b)
}

func TestRatchetCompare(t *testing.T) {
	one := new(SpiralRatchet)
	*one = zero(shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33"))
	two := one.Copy()
	two.Inc()
	twentyFiveThousand := one.Copy()
	twentyFiveThousand.IncBy(25000)

	oneHunderdThousand := one.Copy()
	oneHunderdThousand.IncBy(100000)

	cases := []struct {
		a, b             *SpiralRatchet
		maxSteps, expect int
	}{
		{a: one, b: one, maxSteps: 0, expect: 0},
		{a: one, b: two, maxSteps: 1, expect: -1},
		{a: two, b: one, maxSteps: 1, expect: 1},
		{a: two, b: one, maxSteps: 1, expect: 1},
		{a: twentyFiveThousand, b: one, maxSteps: 10, expect: 25000},
		{a: one, b: oneHunderdThousand, maxSteps: 10, expect: -100000},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			got, err := c.a.Compare(*c.b, 1)
			require.Nil(t, err)
			assert.Equal(t, c.expect, got)
		})
	}

	unrelated := new(SpiralRatchet)
	*unrelated = zero(shasumFromHex("500b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33"))

	_, err := one.Compare(*unrelated, 100000)
	assert.ErrorIs(t, err, ErrUnknownRatchetRelation)
}

func TestRatchetEqual(t *testing.T) {
	a := zero(shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33"))
	b := zero(shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33"))
	c := zero(shasumFromHex("0000000000000000000000000000000000000000000000000000000000000000"))

	if !a.Equal(b) {
		t.Errorf("unexpected inequality. a: %q b: %q", a, b)
	}

	if b.Equal(c) {
		t.Errorf("unexpected equality: a: %q b:%q", a, b)
	}
}

func TestCompliment(t *testing.T) {
	zeros := [32]byte{}
	ones := bytes.Repeat([]byte{255}, 32)
	cmp := compliement(zeros)
	if !bytes.Equal(cmp[:], ones) {
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
		"mediumCursor": hex.EncodeToString([]byte{r.mediumCounter}),
		"small":        hex.EncodeToString(r.small[:]),
		"smallCursor":  hex.EncodeToString([]byte{r.smallCounter}),
	}
}

func BenchmarkRatchetAdd256(b *testing.B) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	for i := 0; i < b.N; i++ {
		r := zero(seed)
		for i := 0; i < 255; i++ {
			r.Inc()
		}
	}
}

func BenchmarkRatchetDeserializeAdd1(b *testing.B) {
	seed := shasumFromHex("600b56e66b7d12e08fd58544d7c811db0063d7aa467a1f6be39990fed0ca5b33")
	var r *SpiralRatchet
	*r = zero(seed)
	// advance ratchet a bunch
	for i := 0; i < 125; i++ {
		r.Inc()
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
		r.Inc()
	}
}
