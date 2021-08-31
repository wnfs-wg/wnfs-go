package base

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPath(t *testing.T) {
	p, err := NewPath("public/baz.txt")
	if err != nil {
		t.Fatal(err)
	}

	got, tail := p.Shift()
	want := "public"
	if want != got {
		t.Errorf("result mismatch. want: %q got: %q", want, got)
	}
	wantTail := Path{"baz.txt"}
	if diff := cmp.Diff(wantTail, tail); diff != "" {
		t.Errorf("result mismatch, (-want +got):\n%s", diff)
	}

	got, tail = tail.Shift()
	want = "baz.txt"
	if want != got {
		t.Errorf("result mismatch. want: %q got: %q", want, got)
	}
	if tail != nil {
		t.Errorf("expected tail to equal nil. got: %v", tail)
	}
}
