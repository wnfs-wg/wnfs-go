package base

import (
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/ipfs/go-cid"
)

func TestSkeletonCBOR(t *testing.T) {
	sk := Skeleton{
		"a": SkeletonInfo{},
		"b": SkeletonInfo{
			Cid:      mustCid("QmSyDX5LYTiwQi861F5NAwdHrrnd1iRGsoEvCyzQMUyZ4W"),
			Userland: mustCid("QmSyDX5LYTiwQi861F5NAwdHrrnd1iRGsoEvCyzQMUyZ4W"),
			Metadata: mustCid("QmSyDX5LYTiwQi861F5NAwdHrrnd1iRGsoEvCyzQMUyZ4W"),
			IsFile:   false,
		},
	}

	f, err := sk.CBORFile()
	if err != nil {
		t.Fatal(err)
	}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	got := Skeleton{}
	if err := DecodeCBOR(data, &got); err != nil {
		t.Fatal(err)
	}

	cmpCid := func(a, b cid.Cid) bool { return a.Equals(b) }
	if diff := cmp.Diff(sk, got, cmp.Comparer(cmpCid)); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
}

func mustCid(s string) cid.Cid {
	id, err := cid.Parse(s)
	if err != nil {
		panic(err)
	}
	return id
}
