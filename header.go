package wnfs

import (
	"io/fs"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

const (
	metadataLinkName = "metadata"
	skeletonLinkName = "skeleton"
	prettyLinkName   = "p"
	previousLinkName = "previous"
	userlandLinkName = "userland"
)

const (
	unixNodeTypeRaw       = "raw"
	unixNodeTypeDirectory = "dir"
	unixNodeTypeFile      = "file"
	unixNodeTypeMetadata  = "metadata"
	unixNodeTypeSymlink   = "symlink"
	unixNodeTypeHAMTShard = "hamtShard"
)

type SemVer string

type UnixMeta struct {
	Mtime int64
	Ctime int64
	Mode  uint32
	Type  string
}

func NewUnixMeta(isFile bool) map[string]interface{} {
	ts := Timestamp().Unix()
	mode := 644
	t := unixNodeTypeFile
	if !isFile {
		mode = 755
		t = unixNodeTypeDirectory
	}

	return map[string]interface{}{
		"mtime": ts,
		"ctime": ts,
		"mode":  uint32(mode),
		"type":  t,
	}
}

type Metadata struct {
	UnixMeta *UnixMeta
	IsFile   bool
	Version  SemVer
}

func loadMetadata(store mdstore.MerkleDagStore, id cid.Cid) (*Metadata, error) {
	d, err := mdstore.GetBlockBytes(store, id)
	if err != nil {
		return nil, err
	}

	md := &Metadata{}
	err = decodeCBOR(d, md)
	return md, err
}

func (md Metadata) CBORFile(encKey *string) (fs.File, error) {
	buf, err := encodeCBOR(md, encKey)
	if err != nil {
		return nil, err
	}

	return &memfile{
		fi: &fsFileInfo{
			name:  metadataLinkName,
			size:  int64(buf.Len()),
			mode:  0755,
			mtime: Timestamp(),
		},
		buf: buf,
	}, nil
}
