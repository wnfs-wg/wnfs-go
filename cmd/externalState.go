package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
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
	path    string
	RootCID cid.Cid
}

func LoadOrCreateExternalState(path string) (*ExternalState, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("creating external state file: %q\n", path)
			s := &ExternalState{path: path}
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
	return s, nil
}

func (s *ExternalState) Write() error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(s.path, data, 0755)
}
