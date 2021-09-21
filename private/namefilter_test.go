package private

import (
	"testing"

	"github.com/qri-io/wnfs-go/bloom"
)

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

	identity := IdentityBareNamefilter()
	root, err := NewBareNamefilter(identity, inumber)
	if err != nil {
		t.Fatal(err)
	}

	knf, err := AddKey(root, childKey)
	if err != nil {
		t.Fatal(err)
	}

	f, err := bloom.DecodeBase64(string(knf))
	if err != nil {
		t.Fatal(err)
	}

	if !f.Has(childKey[:]) {
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
