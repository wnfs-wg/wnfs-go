package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
)

const stateFilename = "wnfs.json"

func ExternalStatePath() (string, error) {
	if path := os.Getenv("WNFS_PATH"); path != "" {
		return path, nil
	}

	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	return filepath.Join(pwd, stateFilename), nil
}

type ExternalState struct {
	path    string
	RootCID cid.Cid
}

func LoadOrCreateExternalState(path string) (*ExternalState, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
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
