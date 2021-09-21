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
