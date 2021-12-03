package mockblocks

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	flatfs "github.com/ipfs/go-ds-flatfs"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

func NewOfflineMemBlockservice() blockservice.BlockService {
	return blockservice.New(NewMemBlockstore(), nil)
}

func NewOfflineFileBlockservice(tmpFilePrefix string) (bserv blockservice.BlockService, cleanup func(), err error) {
	bs, cleanup, err := NewFileBlockstore(tmpFilePrefix)
	if err != nil {
		return nil, nil, err
	}
	return blockservice.New(bs, nil), cleanup, nil
}

type memBlockstore struct {
	data  map[cid.Cid]block.Block
	stats blockstoreStats
}

var _ blockstore.Blockstore = (*memBlockstore)(nil)

func NewMemBlockstore() *memBlockstore {
	return &memBlockstore{make(map[cid.Cid]block.Block), blockstoreStats{}}
}

func (mb *memBlockstore) DeleteBlock(id cid.Cid) error {
	return nil
}

func (mb *memBlockstore) Has(c cid.Cid) (bool, error) {
	_, has := mb.data[c]
	return has, nil
}

// GetSize returns the CIDs mapped BlockSize
func (mb *memBlockstore) GetSize(cid.Cid) (int, error) {
	return 0, fmt.Errorf("unfinished: memBlockstore.GetSize")
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (mb *memBlockstore) PutMany(bs []block.Block) error {
	for _, blk := range bs {
		mb.data[blk.Cid()] = blk
	}
	return nil
}

// AllKeysChan returns a channel from which
// the CIDs in the memBlockstore can be read. It should respect
// the given context, closing the channel if it becomes Done.
func (mb *memBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	cids := make(chan cid.Cid)
	go func() {
		for id := range mb.data {
			cids <- id
		}
		close(cids)
	}()
	return cids, nil
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (mb *memBlockstore) HashOnRead(enabled bool) {
	// noop
}

func (mb *memBlockstore) Get(c cid.Cid) (block.Block, error) {
	mb.stats.evtcntGet++
	d, ok := mb.data[c]
	if ok {
		return d, nil
	}
	return nil, fmt.Errorf("Not Found")
}

func (mb *memBlockstore) Put(b block.Block) error {
	mb.stats.evtcntPut++
	if _, exists := mb.data[b.Cid()]; exists {
		mb.stats.evtcntPutDup++
	}
	mb.data[b.Cid()] = b
	return nil
}

type blockstoreStats struct {
	evtcntGet    int
	evtcntPut    int
	evtcntPutDup int
}

func (mb *memBlockstore) totalBlockSizes() int {
	sum := 0
	for _, v := range mb.data {
		sum += len(v.RawData())
	}
	return sum
}

func NewFileBlockstore(prefix string) (bs blockstore.Blockstore, cleanup func(), err error) {
	if prefix == "" {
		prefix = "mdstore-file-blockstore"
	}
	tdir, err := ioutil.TempDir("", prefix)
	if err != nil {
		return nil, nil, err
	}

	blocksDir := filepath.Join(tdir, "blocks")
	os.Mkdir(blocksDir, os.ModePerm)

	ds, err := flatfs.CreateOrOpen(blocksDir, flatfs.IPFS_DEF_SHARD, true)
	if err != nil {
		return nil, nil, err
	}

	return blockstore.NewBlockstoreNoPrefix(ds), func() { os.RemoveAll(tdir) }, nil
}
