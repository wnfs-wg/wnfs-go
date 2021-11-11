package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	cid "github.com/ipfs/go-cid"
	wnfs "github.com/qri-io/wnfs-go"
	wnipfs "github.com/qri-io/wnfs-go/cmd/ipfs"
	mdstore "github.com/qri-io/wnfs-go/mdstore"
	ratchet "github.com/qri-io/wnfs-go/ratchet"
)

const (
	repoDirname      = ".wnfs"
	stateFilename    = "wnfs-go.json"
	ratchetsFilename = "ratchets.json"
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
	ds    mdstore.MerkleDagStore
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

	ipfsPath := filepath.Join(path, "ipfs")
	if _, err := os.Stat(filepath.Join(ipfsPath, "config")); os.IsNotExist(err) {
		if err := os.MkdirAll(ipfsPath, 0755); err != nil {
			return nil, fmt.Errorf("creating ipfs repo: %w\n", err)
		}
		fmt.Printf("creating ipfs repo at %s ... ", ipfsPath)
		if err = wnipfs.InitRepo(ipfsPath, ""); err != nil {
			return nil, err
		}
		fmt.Println("done")
	}

	store, err := wnipfs.NewFilesystem(ctx, map[string]interface{}{
		"path": ipfsPath,
	})

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

	var fs wnfs.WNFS
	if state.RootCID.Equals(cid.Cid{}) {
		fmt.Printf("creating new wnfs filesystem...")
		if fs, err = wnfs.NewEmptyFS(ctx, store, rs, state.RootKey); err != nil {
			return nil, fmt.Errorf("error: creating empty WNFS: %s\n", err)
		}
		fmt.Println("done")
	} else {
		if fs, err = wnfs.FromCID(ctx, store, rs, state.RootCID, state.RootKey, state.PrivateRootName); err != nil {
			return nil, fmt.Errorf("error: opening WNFS CID %s:\n%s\n", state.RootCID, err.Error())
		}
	}

	return &Repo{
		path:  path,
		ds:    store,
		fs:    fs,
		rs:    rs,
		state: state,
	}, nil
}

func (r *Repo) DagStore() mdstore.MerkleDagStore { return r.ds }
func (r *Repo) RatchetStore() ratchet.Store      { return r.rs }
func (r *Repo) WNFS() wnfs.WNFS                  { return r.fs }
func (r *Repo) Close() error {
	r.state.RootCID = r.fs.Cid()
	r.state.RootKey = r.fs.RootKey()
	var err error
	r.state.PrivateRootName, err = r.fs.PrivateName()
	if err != nil {
		return fmt.Errorf("reading private name: %w", err)
	}
	fmt.Printf("writing root cid: %s...", r.state.RootCID)
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
