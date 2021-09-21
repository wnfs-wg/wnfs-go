package bloom

import "testing"

func TestBasic(t *testing.T) {
	el := []byte("👋")
	f := &Filter{}
	if f.Has(el) {
		t.Errorf("expected new set to not have element")
	}

	f.Add(el)
	if !f.Has(el) {
		t.Errorf("set should have element after adding")
	}

	if f.Has([]byte("👋👋")) {
		t.Error("similar string should not be in set")
	}
}

func TestBase64Coding(t *testing.T) {
	f := &Filter{}
	f.Add([]byte("element"))
	s := f.EncodeBase64()
	got, err := DecodeBase64(s)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Equals(*got) {
		t.Errorf("expected serialization round trip to equal original filter")
	}
}

func BenchmarkSaturation(b *testing.B) {
	var f *Filter
	empty := &Filter{}

	for i := 0; i < b.N; i++ {
		f = empty
		f.Saturate()
	}
}
