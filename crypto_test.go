package wnfs

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"
)

func TestCryptoCoding(t *testing.T) {
	plaintext := strings.Repeat("oh, hello!", 24512)
	t.Logf("plaintext length: %d", len(plaintext))
	key := testRootKey

	ef, err := newEncryptingFile(strings.NewReader(plaintext), key, fsFileInfo{})
	if err != nil {
		t.Fatal(err)
	}

	encrypted, err := ioutil.ReadAll(ef)
	if err != nil {
		t.Fatal(err)
	}

	// um, not exactly an industrial grade test
	if plaintext == string(encrypted) {
		t.Errorf("plaintext cannot equal encrypted text")
	}

	dr, err := newDecryptingReader(bytes.NewBuffer(encrypted), key)
	if err != nil {
		t.Fatal(err)
	}

	pt2, err := ioutil.ReadAll(dr)
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
