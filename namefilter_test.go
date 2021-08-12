package wnfs

import (
	"bytes"
	"testing"

	bloom "github.com/bits-and-blooms/bloom/v3"
)

func TestNameFilterParams(t *testing.T) {
	f := bloom.New(47, 30)
	f.Add(bytes.Repeat([]byte{0}, 32))
	buf := &bytes.Buffer{}
	f.WriteTo(buf)
	if len(buf.Bytes()) != 32 {
		t.Errorf("expect bloom filter to be 32 bytes in length. got: %d", len(buf.Bytes()))
	}
	base := ToBase64(f)
	if len(base) != 32 {
		t.Errorf("expect base64 URL filter to be 32 bytes in length. got: %d\n%s", len(base), base)
	}
}

func TestNamefilter(t *testing.T) {
	var (
		childKey Key = [32]byte{
			1, 1, 1, 1, 1, 1, 1, 1,
			1, 1, 1, 1, 1, 1, 1, 1,
			1, 1, 1, 1, 1, 1, 1, 1,
			1, 1, 1, 1, 1, 1, 1, 1,
		}
		inumber = NewINumber()
	)

	// fil := bloom.New(filterSize, hashCount)
	// fil.Add([]byte(rootKey))
	// root := BareNamefilter(ToBase64(fil))
	// // root, err := CreateBare(rootKey)
	// // if err != nil {
	// // 	t.Fatal(err)
	// // }
	identity := identityBareNamefilter()
	root, err := NewBareNamefilter(identity, inumber)
	if err != nil {
		t.Fatal(err)
	}

	knf, err := AddKey(root, childKey)
	if err != nil {
		t.Fatal(err)
	}

	f, err := FromBase64(string(knf))
	if err != nil {
		t.Fatal(err)
	}

	if !f.Test(childKey[:]) {
		t.Errorf("expected childKey to be present in namefilter")
	}

	// ch, err := AddToBare(root, childKey[:])
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// t.Log(len(ch))

	// rev, err := AddRevision(ch, childKey, revNumber)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// t.Log(len(rev))

	// snf, err := Saturate(rev)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// t.Log(len(snf))

	// secretName, err := ToPrivateName(knf)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// // construct a filter from the garbage secretName spits out
	// sf, err := FromBase64(string(secretName))
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// if sf.Test(childKey[:]) {
	// 	t.Errorf("expected childKey to NOT be present in namefilter!")
	// }
	// if sf.Test([]byte(rootKey)) {
	// 	t.Errorf("expected rootKey to NOT be present in namefilter!")
	// }
}
