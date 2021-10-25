package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/qri-io/wnfs-go"
	"github.com/qri-io/wnfs-go/ratchet"
)

const stateFilename = "wnfs-go.json"

func ExternalStatePath() (string, error) {
	if path := os.Getenv("WNFS_STATE_PATH"); path != "" {
		return path, nil
	}

	configDir, err := configDirPath()
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(configDir, 0755); err != nil {
		return "", err
	}

	return filepath.Join(configDir, stateFilename), nil
}

func configDirPath() (string, error) {
	home, err := homedir.Dir()
	if err != nil {
		return home, err
	}
	return filepath.Join(home, ".config", "wnfs"), nil
}

type ExternalState struct {
	path string
	rs   ratchet.Store

	RootCID         cid.Cid
	RootKey         wnfs.Key
	PrivateRootName wnfs.PrivateName
}

func LoadOrCreateExternalState(ctx context.Context, path string) (*ExternalState, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("creating external state file: %q\n", path)
			rs, err := ratchet.NewStore(ctx, filepath.Join(filepath.Dir(path), "ratchets.json"))
			if err != nil {
				return nil, err
			}
			s := &ExternalState{
				path:    path,
				rs:      rs,
				RootKey: wnfs.NewKey(),
			}
			err = s.Write()
			return s, err
		}
		return nil, err
	}

	s := &ExternalState{}
	if err := json.Unmarshal(data, s); err != nil {
		return nil, err
	}
	s.path = path
	if s.rs, err = ratchet.NewStore(ctx, filepath.Join(filepath.Dir(path), "ratchets.json")); err != nil {
		return nil, err
	}

	// construct a key if one doesn't exist
	if s.RootKey.IsEmpty() {
		fmt.Println("creating new root key")
		s.RootKey = wnfs.NewKey()
		return s, s.Write()
	}
	return s, nil
}

func (s *ExternalState) RatchetStore() ratchet.Store {
	return s.rs
}

func (s *ExternalState) Write() error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(s.path, data, 0755)
}
