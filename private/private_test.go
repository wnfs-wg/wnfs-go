package private

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	hamt "github.com/filecoin-project/go-hamt-ipld/v3"
	cid "github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	golog "github.com/ipfs/go-log"
	"github.com/multiformats/go-multihash"
	base "github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/private/ratchet"
	"github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func init() {
	if lvl := os.Getenv("WNFS_LOGGING"); lvl != "" {
		golog.SetLogLevel("wnfs", lvl)
	}
}

var testRootKey Key = [32]byte{
	1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	1, 2,
}

func TestCryptoFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemTestPrivateStore(ctx, t)

	plaintext := strings.Repeat("oh hello. ", 1235340)
	key := testRootKey[:]

	res, err := store.PutEncryptedFile(base.NewMemfileBytes("", []byte(plaintext)), key)
	require.Nil(t, err)

	f, err := store.GetEncryptedFile(res.Cid, key)
	require.Nil(t, err)

	pt2, err := ioutil.ReadAll(f)
	require.Nil(t, err)

	if len(plaintext) != len(pt2) {
		t.Errorf("decoded length mismatch. want: %d got: %d", len(plaintext), len(pt2))
	}

	if plaintext != string(pt2) {
		t.Errorf("result mismatch:\nwant: %q\ngot:  %q", plaintext, string(pt2))
	}
}

func TestHistory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemTestPrivateStore(ctx, t)

	root, err := NewEmptyRoot(ctx, store, "private", testRootKey)
	require.Nil(t, err)

	_, err = root.Add(base.MustPath("hi.txt"), base.NewMemfileBytes("hi.txt", []byte("oh hello")))
	require.Nil(t, err)

	_, err = root.Put()
	require.Nil(t, err)

	_, err = root.Add(base.MustPath("hi.txt"), base.NewMemfileBytes("hi.txt", []byte("oh hello 2")))
	require.Nil(t, err)

	_, err = root.Put()
	require.Nil(t, err)

	file, err := root.Get(base.MustPath("hi.txt"))
	require.Nil(t, err)

	hist, err := file.(privateNode).History(ctx, -1)
	require.Nil(t, err)

	t.Logf("%#v", hist)
}

func TestHeaderCoding(t *testing.T) {
	hash, err := multihash.Sum([]byte("hi"), base.DefaultMultihashType, -1)
	require.Nil(t, err)
	content := cid.NewCidV1(cid.DagCBOR, hash)

	h := Header{
		Info: HeaderInfo{
			WNFS:  base.LatestVersion,
			Type:  base.NTFile,
			Mode:  base.ModeDefault,
			Ctime: time.Now().Unix(),
			Mtime: time.Now().Unix(),
			Size:  35,

			INumber:        NewINumber(),
			BareNamefilter: IdentityBareNamefilter(),
			Ratchet:        ratchet.NewSpiral().Encode(),
		},
		ContentID: content,
	}
	blk, err := h.encryptHeaderBlock(testRootKey)
	require.Nil(t, err)

	got, err := decodeHeaderBlock(blk, testRootKey)
	require.Nil(t, err)

	assert.Equal(t, h, got)
}

func TestPrivateLinkBlockCoding(t *testing.T) {
	hash, err := multihash.Sum([]byte("hi"), base.DefaultMultihashType, -1)
	require.Nil(t, err)
	fooCid := cid.NewCidV1(cid.DagCBOR, hash)

	links := PrivateLinks{
		"foo": PrivateLink{Link: base.Link{Name: "foo", Cid: fooCid, Size: 5, Mtime: 20}, Key: testRootKey, Pointer: Name("apples")},
	}

	blk, err := links.marshalEncryptedBlock(testRootKey)
	require.Nil(t, err)

	got, err := unmarshalPrivateLinksBlock(blk, testRootKey)
	require.Nil(t, err)

	assert.Equal(t, links, got)
}

func TestPrivateBlockWriting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemTestPrivateStore(ctx, t)

	root, err := NewEmptyRoot(ctx, store, "private", testRootKey)
	require.Nil(t, err)

	_, err = root.Add(base.MustPath("hi.txt"), base.NewMemfileBytes("hi.txt", []byte("oh hello")))
	require.Nil(t, err)

	pn, err := root.PrivateName()
	require.Nil(t, err)

	root, err = LoadRoot(ctx, store, "", root.Key(), pn)
	require.Nil(t, err)

	allBlocksPresent(t, root.cid, store)
}

func allBlocksPresent(t *testing.T, id cid.Cid, store Store) {
	var (
		ctx = context.Background()
		n   ipld.Node
		err error
	)
	// TODO(b5): need to do this switch on codec because go-merkledag package no
	// longer supports dag-cbor out of the box >:(
	// More reasons to switch away from go-ipld libraries
	switch id.Prefix().Codec {
	case 0x71: // dag-cbor
		blk, err := store.Blockservice().GetBlock(ctx, id)
		require.Nil(t, err)
		n, err = ipldcbor.DecodeBlock(blk)
		require.Nil(t, err)
	default:
		n, err = store.DAGService().Get(ctx, id)
		require.Nil(t, err)
	}
	require.Nil(t, err)

	log.Debugw("CopyBlocks", "cid", id, "len(links)", len(n.Links()))
	for _, l := range n.Links() {
		allBlocksPresent(t, l.Cid, store)
	}

	_, err = store.Blockservice().Blockstore().Get(ctx, id)
	require.Nil(t, err)
}

type fataler interface {
	Name() string
	Helper()
	Fatal(args ...interface{})
}

func mustHistCids(t *testing.T, tree *Tree, path base.Path) []cid.Cid {
	t.Helper()
	f, err := tree.Get(path)
	require.Nil(t, err)

	log, err := f.(base.Node).History(context.Background(), -1)
	require.Nil(t, err)
	ids := make([]cid.Cid, len(log))
	for i, l := range log {
		ids[i] = l.Cid
	}
	return ids
}

func mustDirChildren(t *testing.T, dir base.Tree, ch []string) {
	t.Helper()
	ents, err := dir.ReadDir(-1)
	require.Nil(t, err)

	got := make([]string, 0, len(ents))
	for _, ch := range ents {
		got = append(got, ch.Name())
	}

	assert.Equal(t, ch, got)
}

func mustFileContents(t *testing.T, dir base.Tree, path, content string) {
	t.Helper()
	f, err := dir.Get(base.MustPath(path))
	require.Nil(t, err)
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	require.Nil(t, err)

	assert.Equal(t, content, string(data))
}

func printHamt(label string, h *hamt.Node) {
	ctx := context.Background()
	fmt.Printf("HAMT: %s\n", label)
	h.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		fmt.Println(k)
		return nil
	})
}
