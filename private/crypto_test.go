package private

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/qri-io/wnfs-go/base"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
	mdstoremock "github.com/qri-io/wnfs-go/mdstore/mock"
)

var testRootKey Key = [32]byte{
	1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	1, 2,
}

func TestCryptoFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := mdstore.NewPrivateStore(ctx, mdstoremock.NewOfflineMemBlockservice())
	if err != nil {
		t.Fatal(err)
	}

	plaintext := strings.Repeat("oh hello. ", 1235340)
	key := testRootKey[:]

	res, err := store.PutEncryptedFile(base.NewMemfileBytes("", []byte(plaintext)), key)
	if err != nil {
		t.Fatal(err)
	}

	f, err := store.GetEncryptedFile(res.Cid, key)
	if err != nil {
		t.Fatal(err)
	}

	pt2, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	if len(plaintext) != len(pt2) {
		t.Errorf("decoded length mismatch. want: %d got: %d", len(plaintext), len(pt2))
	}

	if plaintext != string(pt2) {
		t.Errorf("result mismatch:\nwant: %q\ngot:  %q", plaintext, string(pt2))
	}
}
