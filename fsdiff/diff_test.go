package fsdiff

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/stretchr/testify/require"
)

func TestTree(t *testing.T) {
	fs := os.DirFS("testdata/one")
	got, err := Tree("a", "b", fs, fs)
	if err != nil {
		t.Fatal(err)
	}

	expect := &Delta{
		Type: DTChange,
		Name: ".",
		Deltas: []*Delta{
			{Type: DTAdd, Name: "four.txt"},
			{Type: DTChange, Name: "sub", Deltas: []*Delta{
				{Type: DTAdd, Name: "three.txt"},
				{Type: DTChange, Name: "one.txt"},
				{Type: DTRemove, Name: "two.txt"},
			}},
		},
	}

	if diff := cmp.Diff(expect, got); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestUnix(t *testing.T) {
	aFs := os.DirFS("testdata/one/a")
	bFs := os.DirFS("testdata/one/b")
	got, err := Unix(".", ".", aFs, bFs)
	require.Nil(t, err)

	dmp := diffmatchpatch.New()
	for _, f := range got {
		fmt.Printf("%s\n", f.Path)
		fmt.Println(dmp.DiffPrettyText(f.Diff))
	}
}
