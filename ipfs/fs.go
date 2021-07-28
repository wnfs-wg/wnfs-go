package ipfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/ipfs/go-cid"
	ipfs_config "github.com/ipfs/go-ipfs-config"
	files "github.com/ipfs/go-ipfs-files"
	ipfs_commands "github.com/ipfs/go-ipfs/commands"
	"github.com/ipfs/go-ipfs/core"
	ipfs_core "github.com/ipfs/go-ipfs/core"
	coreapi "github.com/ipfs/go-ipfs/core/coreapi"
	ipfs_corehttp "github.com/ipfs/go-ipfs/core/corehttp"
	ipfsrepo "github.com/ipfs/go-ipfs/repo"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	cbor "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	caopts "github.com/ipfs/interface-go-ipfs-core/options"
	corepath "github.com/ipfs/interface-go-ipfs-core/path"
	multihash "github.com/multiformats/go-multihash"
	"github.com/qri-io/qfs"
	"github.com/qri-io/wnfs-go/mdstore"
)

var (
	log = logging.Logger("wn-ipfs")
	// ErrNoRepoPath is returned when no repo path is provided in the config
	ErrNoRepoPath = errors.New("must provide a repo path to initialize an ipfs filesystem")
)

// FilestoreType uniquely identifies this filestore
const FilestoreType = "ipfs"

type Filestore struct {
	ctx  context.Context
	cfg  *StoreCfg
	node *core.IpfsNode
	capi coreiface.CoreAPI

	doneCh  chan struct{}
	doneErr error
}

func (fs *Filestore) StoreType() string { return FilestoreType }

// NewFilesystem creates a new local filesystem PathResolver
// with no options
func NewFilesystem(ctx context.Context, cfgMap map[string]interface{}) (mdstore.MerkleDagStore, error) {
	cfg, err := mapToConfig(cfgMap)
	if err != nil {
		return nil, err
	}

	if cfg.BuildCfg.ExtraOpts == nil {
		cfg.BuildCfg.ExtraOpts = map[string]bool{}
	}
	cfg.BuildCfg.ExtraOpts["pubsub"] = cfg.EnablePubSub

	if cfg.Path == "" && cfg.URL == "" {
		return nil, ErrNoRepoPath
	} else if cfg.URL != "" {
		return nil, fmt.Errorf("unfinished: ipfs coreAPI via HTTP")
		// return qipfs_http.NewFilesystem(map[string]interface{}{"url": cfg.URL})
	}

	if err := LoadIPFSPluginsOnce(cfg.Path); err != nil {
		return nil, err
	}

	cfg.Repo, err = openRepo(ctx, cfg)
	if err != nil {
		if cfg.URL != "" && err == errRepoLock {
			// if we cannot get a repo, and we have a fallback APIAdder
			// attempt to create and return an `qipfs_http` filesystem istead
			// return qipfs_http.NewFilesystem(map[string]interface{}{"url": cfg.URL})
			return nil, fmt.Errorf("unfinished: ipfs coreAPI via HTTP")
		}
		log.Errorf("opening %q: %s", cfg.Path, err)
		return nil, err
	}

	node, err := core.NewNode(ctx, &cfg.BuildCfg)
	if err != nil {
		return nil, fmt.Errorf("qipfs: error creating ipfs node: %w", err)
	}

	if cfg.DisableBootstrap {
		repoCfg, err := node.Repo.Config()
		if err != nil {
			return nil, err
		}
		repoCfg.Bootstrap = []string{}
	}

	if len(cfg.AdditionalSwarmListeningAddrs) != 0 {
		repoCfg, err := node.Repo.Config()
		if err != nil {
			return nil, err
		}
		repoCfg.Addresses.Swarm = append(repoCfg.Addresses.Swarm, cfg.AdditionalSwarmListeningAddrs...)
	}

	capi, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	fst := &Filestore{
		ctx:    ctx,
		cfg:    cfg,
		node:   node,
		capi:   capi,
		doneCh: make(chan struct{}),
	}

	go fst.handleContextClose(ctx)
	return fst, nil
}

// NewFilesystemFromNode wraps an existing IPFS node with a qfs.Filesystem
func NewFilesystemFromNode(ctx context.Context, node *core.IpfsNode) (mdstore.MerkleDagStore, error) {
	capi, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}

	fst := &Filestore{
		ctx:    ctx,
		node:   node,
		capi:   capi,
		doneCh: make(chan struct{}),
	}

	go fst.handleContextClose(ctx)
	return fst, nil
}

// Type distinguishes this filesystem from others by a unique string prefix
func (fst Filestore) Type() string {
	return FilestoreType
}

func (fs *Filestore) GetNode(id cid.Cid, path ...string) (mdstore.DagNode, error) {
	if len(path) > 0 {
		return nil, fmt.Errorf("unsupported: path values on ipfs.Filestore.GetNode")
	}
	node, err := fs.capi.Dag().Get(fs.ctx, id)
	if err != nil {
		return nil, err
	}

	size, err := node.Size()
	if err != nil {
		return nil, err
	}

	links := mdstore.NewLinks()
	for _, link := range node.Links() {
		links.Add(mdstore.Link{
			Name: link.Name,
			Cid:  link.Cid,
			Size: int64(link.Size),
		})
	}

	return &ipfsDagNode{
		id:    id,
		size:  int64(size),
		node:  node,
		links: links,
	}, nil
}

func (fs *Filestore) PutNodeWithData(obj map[string]interface{}, links mdstore.Links) (mdstore.PutResult, error) {
	// node := unixfs.EmptyDirNode()
	// node := &merkledag.ProtoNode{}
	// node.SetData(unixfs.FolderPBData())
	// node.SetCidBuilder(cid.V1Builder{
	// 	Codec:    cid.DagProtobuf,
	// 	MhType:   multihash.SHA2_256,
	// 	MhLength: -1,
	// })

	// Make an object
	for name, lnk := range links.Map() {
		obj[name] = lnk.IPLD().Cid
		// node.AddRawLink(name, lnk.IPLD())
	}

	// data, err := json.Marshal(obj)
	// if err != nil {
	// 	return mdstore.PutResult{}, err
	// }

	// Parse it into an ipldcbor node
	// node, err := cbor.FromJSON(bytes.NewBuffer(data), multihash.SHA2_256, -1)
	// if err != nil {
	// 	return mdstore.PutResult{}, err
	// }
	node, err := cbor.WrapObject(obj, multihash.SHA2_256, -1)
	if err != nil {
		return mdstore.PutResult{}, err
	}

	// fmt.Printf("%x\n", node.RawData())

	err = fs.capi.Dag().Add(fs.ctx, node)
	if err != nil {
		return mdstore.PutResult{}, err
	}
	size, err := node.Size()
	if err != nil {
		return mdstore.PutResult{}, err
	}

	return mdstore.PutResult{
		Cid:  node.Cid(),
		Size: int64(size),
	}, err
}

func (fs *Filestore) PutNode(links mdstore.Links) (mdstore.PutResult, error) {
	obj := map[string]interface{}{}
	return fs.PutNodeWithData(obj, links)
}

func (fs *Filestore) GetBlock(id cid.Cid) (io.Reader, error) {
	return fs.capi.Block().Get(fs.ctx, corepath.IpldPath(id))
}

func (fs *Filestore) PutBlock(d []byte) (id cid.Cid, err error) {
	bs, err := fs.capi.Block().Put(fs.ctx, bytes.NewBuffer(d), caopts.Block.Hash(multihash.SHA1, -1), caopts.Block.Format("raw"))
	if err != nil {
		return cid.Cid{}, err
	}
	return bs.Path().Root(), nil
}

func (fs *Filestore) PutFile(f fs.File) (mdstore.PutResult, error) {
	path, err := fs.capi.Unixfs().Add(fs.ctx, files.NewReaderFile(f), caopts.Unixfs.CidVersion(1))
	if err != nil {
		return mdstore.PutResult{}, err
	}

	storedFile, err := fs.capi.Unixfs().Get(fs.ctx, path)
	if err != nil {
		return mdstore.PutResult{}, err
	}

	size, err := storedFile.Size()
	if err != nil {
		return mdstore.PutResult{}, err
	}

	return mdstore.PutResult{
		Cid:  path.Root(),
		Size: size,
	}, nil
}

func (fs *Filestore) GetFile(root cid.Cid, path ...string) (io.ReadCloser, error) {
	nd, err := fs.capi.Unixfs().Get(fs.ctx, corepath.IpfsPath(root))
	if err != nil {
		return nil, err
	}
	// TODO(b5) - assertion check
	return nd.(io.ReadCloser), nil
}

// Done implements the qfs.ReleasingFilesystem interface
func (fst *Filestore) Done() <-chan struct{} {
	return fst.doneCh
}

// DoneErr returns errors in closing the filesystem
func (fst *Filestore) DoneErr() error {
	return fst.doneErr
}

// CoreAPI exposes the Filestore's CoreAPI interface
func (fst *Filestore) CoreAPI() coreiface.CoreAPI {
	return fst.capi
}

func (fst *Filestore) Online() bool {
	return fst.node.IsOnline
}

func (fst *Filestore) GoOnline(ctx context.Context) error {
	log.Debug("going online")
	cfg := fst.cfg
	cfg.BuildCfg.Online = true
	node, err := core.NewNode(ctx, &cfg.BuildCfg)
	if err != nil {
		return fmt.Errorf("error creating ipfs node: %s\n", err.Error())
	}

	capi, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return err
	}

	*fst = Filestore{
		cfg:  cfg,
		node: node,
		capi: capi,

		doneCh:  fst.doneCh,
		doneErr: fst.doneErr,
	}

	if cfg.EnableAPI {
		go func() {
			if err := fst.serveAPI(); err != nil {
				log.Errorf("error serving IPFS HTTP api: %s", err)
			}
		}()
	}

	return nil
}

// PinsetDifference returns a map of "Recursive"-pinned hashes that are not in
// the given set of hash keys. The returned set is a list of all data
func (fst *Filestore) PinsetDifference(ctx context.Context, set map[string]struct{}) (<-chan string, error) {
	resCh := make(chan string, 10)
	res, err := fst.capi.Pin().Ls(ctx, func(o *caopts.PinLsSettings) error {
		o.Type = "recursive"
		return nil
	})
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(resCh)
	LOOP:
		for {
			select {
			case p, ok := <-res:
				if !ok {
					break LOOP
				}

				str := p.Path().String()
				if _, ok := set[str]; !ok {
					// send on channel if path is not in set
					resCh <- str
				}
			case <-ctx.Done():
				log.Debug(ctx.Err())
				break LOOP
			}
		}
	}()

	return resCh, nil
}

func (fst *Filestore) handleContextClose(ctx context.Context) {
	<-ctx.Done()
	fst.doneErr = ctx.Err()
	log.Debugf("closing repo")

	if err := fst.node.Repo.Close(); err != nil {
		log.Error(err)
	}

	if fsr, ok := fst.node.Repo.(*fsrepo.FSRepo); ok {
		for {
			daemonLocked, err := fsrepo.LockedByOtherProcess(fsr.Path())
			if err != nil {
				log.Error(err)
				break
			} else if daemonLocked {
				log.Errorf("fsrepo is still locked")
				time.Sleep(time.Millisecond * 25)
				continue
			}
			break
		}
		log.Debugf("closed repo at %q", fsr.Path())
	}

	close(fst.doneCh)
}

func openRepo(ctx context.Context, cfg *StoreCfg) (ipfsrepo.Repo, error) {
	if cfg.NilRepo {
		return nil, nil
	}
	if cfg.Repo != nil {
		return nil, nil
	}
	if cfg.Path != "" {
		log.Debugf("opening repo at %q", cfg.Path)
		if daemonLocked, err := fsrepo.LockedByOtherProcess(cfg.Path); err != nil {
			return nil, err
		} else if daemonLocked {
			return nil, errRepoLock
		}
		localRepo, err := fsrepo.Open(cfg.Path)
		if err != nil {
			if err == fsrepo.ErrNeedMigration {
				return nil, ErrNeedMigration
			}
			return nil, fmt.Errorf("error opening local filestore ipfs repository: %w", err)
		}

		return localRepo, nil
	}
	return nil, fmt.Errorf("no repo path to open IPFS fsrepo")
}

// serveAPI makes an IPFS node available over an HTTP api
func (fs *Filestore) serveAPI() error {
	if fs.node == nil {
		return fmt.Errorf("node is required to serve IPFS HTTP API")
	}

	cfg := fs.cfg
	addr := ""
	if cfg.Repo != nil {
		if ipfscfg, err := cfg.Repo.Config(); err == nil {
			// TODO (b5): apparantly ipfs config supports multiple API multiaddrs?
			// I dunno, for now just go with the most likely case of only assigning
			// an address if one string is supplied
			if len(ipfscfg.Addresses.API) == 1 {
				addr = ipfscfg.Addresses.API[0]
			}
		}
	}

	opts := []ipfs_corehttp.ServeOption{
		ipfs_corehttp.GatewayOption(true, "/ipfs", "/ipns"),
		ipfs_corehttp.WebUIOption,
		ipfs_corehttp.CommandsOption(cmdCtx(fs.node, cfg.Path)),
	}

	// TODO (b5): I've added this fmt.Println because the corehttp package includes a println
	// call to the affect of "API server listening on [addr]", which will be confusing to our
	// users. We should chat with the protocol folks about making that print statement mutable
	// or configurable
	fmt.Println("starting IPFS HTTP API:")
	return ipfs_corehttp.ListenAndServe(fs.node, addr, opts...)
}

type ipfsDagNode struct {
	id    cid.Cid
	size  int64
	node  format.Node
	links mdstore.Links
}

var _ mdstore.DagNode = (*ipfsDagNode)(nil)

func (n ipfsDagNode) Size() int64  { return n.size }
func (n ipfsDagNode) Cid() cid.Cid { return n.id }
func (n ipfsDagNode) Raw() []byte  { return n.node.RawData() }
func (n ipfsDagNode) Links() mdstore.Links {
	return n.links
}

type ipfsFile struct {
	path string
	r    io.ReadCloser
}

var _ qfs.File = (*ipfsFile)(nil)

// Read proxies to the response body reader
func (f ipfsFile) Read(p []byte) (int, error) {
	return f.r.Read(p)
}

// Close proxies to the response body reader
func (f ipfsFile) Close() error {
	return f.r.Close()
}

// IsDirectory satisfies the qfs.File interface
func (f ipfsFile) IsDirectory() bool {
	return false
}

// NextFile satisfies the qfs.File interface
func (f ipfsFile) NextFile() (qfs.File, error) {
	return nil, qfs.ErrNotDirectory
}

// FileName returns a filename associated with this file
func (f ipfsFile) FileName() string {
	return filepath.Base(f.path)
}

// FullPath returns the full path used when adding this file
func (f ipfsFile) FullPath() string {
	return f.path
}

// MediaType maps an ipfs CID to a media type. Media types are not yet
// implemented for ipfs files
// TODO (b5) - finish
func (f ipfsFile) MediaType() string {
	return ""
}

// ModTime gets the last time of modification. ipfs files are immutable
// and will always have a ModTime of zero
func (f ipfsFile) ModTime() time.Time {
	return time.Time{}
}

// extracted from github.com/ipfs/go-ipfs/cmd/ipfswatch/main.go
func cmdCtx(node *ipfs_core.IpfsNode, repoPath string) ipfs_commands.Context {
	return ipfs_commands.Context{
		// Online:     true,

		ConfigRoot: repoPath,
		ReqLog:     &ipfs_commands.ReqLog{},
		LoadConfig: func(path string) (*ipfs_config.Config, error) {
			return node.Repo.Config()
		},
		ConstructNode: func() (*ipfs_core.IpfsNode, error) {
			return node, nil
		},
	}
}
