package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	blockservice "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	flatfs "github.com/ipfs/go-ds-flatfs"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	wnfs "github.com/qri-io/wnfs-go"
	private "github.com/qri-io/wnfs-go/private"
	ratchet "github.com/qri-io/wnfs-go/private/ratchet"
	public "github.com/qri-io/wnfs-go/public"
)

const (
	repoDirname        = ".wnfs"
	stateFilename      = "wnfs-go.json"
	ratchetsFilename   = "ratchets.json"
	decryptionFilename = "decryption.json"
)

func RepoPath() (string, error) {
	if path := os.Getenv("WNFS_PATH"); path != "" {
		return path, nil
	}

	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(pwd, repoDirname), nil
}

type Repo struct {
	path  string
	fs    wnfs.WNFS
	rs    ratchet.Store
	dec   private.WritableDecryptionStore
	store public.Store
	state *State
}

func OpenRepo(ctx context.Context) (*Repo, error) {
	path, err := RepoPath()
	if err != nil {
		return nil, err
	}
	return OpenRepoPath(ctx, path)
}

func OpenRepoPath(ctx context.Context, path string) (*Repo, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	ffs, err := flatfs.CreateOrOpen(filepath.Join(path, "flatfs"), flatfs.IPFS_DEF_SHARD, false)
	if err != nil {
		return nil, err
	}
	fbs := blockstore.NewBlockstoreNoPrefix(ffs)
	bserv := blockservice.New(fbs, nil)
	store := public.NewStore(ctx, bserv)

	if err != nil {
		return nil, fmt.Errorf("error: opening IPFS repo: %s\n", err)
	}

	state, err := loadOrCreateState(ctx, filepath.Join(path, stateFilename))
	if err != nil {
		return nil, fmt.Errorf("error: loading external state: %s\n", err)
	}

	rs, err := ratchet.NewStore(ctx, filepath.Join(path, ratchetsFilename))
	if err != nil {
		return nil, err
	}

	dec, err := private.NewDecryptionStore(filepath.Join(path, decryptionFilename))
	if err != nil {
		return nil, err
	}

	var fs wnfs.WNFS
	if state.RootCID.Equals(cid.Cid{}) {
		fmt.Printf("creating new wnfs filesystem...")
		if fs, err = wnfs.NewEmptyFS(ctx, store.Blockservice(), rs, state.RootKey); err != nil {
			return nil, fmt.Errorf("error: creating empty WNFS: %s\n", err)
		}
		fmt.Println("done")
	} else {
		if fs, err = wnfs.FromCID(ctx, store.Blockservice(), rs, state.RootCID, state.RootKey, state.PrivateRootName); err != nil {
			return nil, fmt.Errorf("error: opening WNFS CID %s:\n%s\n", state.RootCID, err.Error())
		}
	}

	return &Repo{
		path:  path,
		store: store,
		fs:    fs,
		rs:    rs,
		dec:   dec,
		state: state,
	}, nil
}

func (r *Repo) Store() public.Store         { return r.store }
func (r *Repo) RatchetStore() ratchet.Store { return r.rs }
func (r *Repo) WNFS() wnfs.WNFS             { return r.fs }
func (r *Repo) Factory() wnfs.Factory {
	return wnfs.Factory{
		BlockService: r.store.Blockservice(),
		Ratchets:     r.rs,
		Decryption:   r.dec,
	}
}

func (r *Repo) Close() error {
	r.state.RootCID = r.fs.Cid()
	r.state.RootKey = r.fs.RootKey()
	var err error
	r.state.PrivateRootName, err = r.fs.PrivateName()
	if err != nil {
		return fmt.Errorf("reading private name: %w", err)
	}
	if err = r.dec.PutDecryptionFields(r.state.RootCID, r.state.PrivateRootName, r.state.RootKey); err != nil {
		return fmt.Errorf("updating decryption store: %w", err)
	}
	fmt.Printf("writing root cid: %s ...", r.state.RootCID)
	if err := r.state.Write(); err != nil {
		fmt.Printf("\n")
		return fmt.Errorf("writing external state: %w", err)
	}
	fmt.Println("done")
	return nil
}

type State struct {
	path            string
	RootCID         cid.Cid
	RootKey         wnfs.Key
	PrivateRootName wnfs.PrivateName
}

func loadOrCreateState(ctx context.Context, path string) (*State, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("creating external state file: %q\n", path)

			s := &State{
				path:    path,
				RootKey: wnfs.NewKey(),
			}
			err = s.Write()
			return s, err
		}
		return nil, err
	}

	s := &State{}
	if err := json.Unmarshal(data, s); err != nil {
		return nil, err
	}
	s.path = path

	// construct a key if one doesn't exist
	if s.RootKey.IsEmpty() {
		fmt.Println("creating new root key")
		s.RootKey = wnfs.NewKey()
		return s, s.Write()
	}
	return s, nil
}

func (s *State) Write() error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(s.path, data, 0755)
}
