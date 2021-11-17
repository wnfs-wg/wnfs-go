package base

import (
	"context"
	"io/fs"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

const (
	UnixNodeTypeRaw       = "raw"
	UnixNodeTypeDirectory = "dir"
	UnixNodeTypeFile      = "file"
	UnixNodeTypeMetadata  = "metadata"
	UnixNodeTypeSymlink   = "symlink"
	UnixNodeTypeHAMTShard = "hamtShard"
)

const (
	ModeDefault = 644
)

type NodeType uint8

const (
	NTFile NodeType = iota
	NTDataFile
	NTDir
	NTSymlink    // reserved for future use
	NTUnixFSFile // reserved for future use
	NTUnixFSDir  // reserved for future use
)

func (nt NodeType) String() string {
	switch nt {
	case NTFile:
		return "file"
	case NTDir:
		return "dir"
	case NTSymlink:
		return "symlink"
	case NTUnixFSFile:
		return "unixFSFile"
	case NTUnixFSDir:
		return "unixFSDir"
	default:
		return "unknown"
	}
}

type SemVer string

type UnixMeta struct {
	Mtime int64
	Ctime int64
	Mode  uint32
	Type  string
}

func NewUnixMeta(isFile bool) *UnixMeta {
	ts := Timestamp().Unix()
	mode := 644
	t := UnixNodeTypeFile
	if !isFile {
		mode = 755
		t = UnixNodeTypeDirectory
	}

	return &UnixMeta{
		Mtime: ts,
		Ctime: ts,
		Mode:  uint32(mode),
		Type:  t,
	}
}

type Metadata struct {
	UnixMeta *UnixMeta
	IsFile   bool
	Version  SemVer
}

func LoadMetadata(ctx context.Context, store mdstore.MerkleDagStore, id cid.Cid) (*Metadata, error) {
	d, err := store.GetBlock(ctx, id)
	if err != nil {
		return nil, err
	}

	md := &Metadata{}
	err = DecodeCBOR(d, md)
	return md, err
}

func (md Metadata) CBORFile() (fs.File, error) {
	buf, err := EncodeCBOR(md)
	if err != nil {
		return nil, err
	}

	return &memfile{
		fi: &FSFileInfo{
			name:  MetadataLinkName,
			size:  int64(buf.Len()),
			mode:  0755,
			mtime: Timestamp(),
		},
		buf: buf,
	}, nil
}
