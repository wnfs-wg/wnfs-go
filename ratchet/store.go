package ratchet

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
)

var ErrRatchetNotFound = fmt.Errorf("ratchet not found")

type Store interface {
	PutRatchet(ctx context.Context, name string, ratchet *Spiral) (updated bool, err error)
	OldestKnownRatchet(ctx context.Context, name string) (*Spiral, error)
	Flush() error
}

type ratchetStore struct {
	ctx   context.Context
	path  string
	lk    sync.Mutex
	cache map[string]*Spiral
}

var _ Store = (*ratchetStore)(nil)

func NewStore(ctx context.Context, path string) (Store, error) {
	s := &ratchetStore{ctx: ctx, path: path, cache: map[string]*Spiral{}}
	err := s.load()
	return s, err
}

func NewMemStore(ctx context.Context) Store {
	return &ratchetStore{ctx: ctx, cache: map[string]*Spiral{}}
}

func (s *ratchetStore) PutRatchet(ctx context.Context, name string, ratchet *Spiral) (updated bool, err error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	if _, exists := s.cache[string(name)]; !exists {
		log.Debugw("writing ratchet", "name", name)
		s.cache[string(name)] = ratchet
		updated = true
	}
	return updated, nil
}

func (s *ratchetStore) OldestKnownRatchet(ctx context.Context, name string) (*Spiral, error) {
	s.lk.Lock()
	defer s.lk.Unlock()
	log.Debugw("get ratchet", "name", name)

	got, exists := s.cache[string(name)]
	if !exists {
		return nil, ErrRatchetNotFound
	}
	return got, nil
}

func (s *ratchetStore) load() error {
	if d, err := ioutil.ReadFile(s.path); err == nil {
		enc := map[string]string{}
		if err := json.Unmarshal(d, &enc); err != nil {
			return fmt.Errorf("reading ratchet store JSON file: %w", err)
		}
		for k, e := range enc {
			r, err := DecodeSpiral(e)
			if err != nil {
				return fmt.Errorf("decoding ratchet at key %q: %w", k, err)
			}
			s.cache[k] = r
		}
		log.Debugw("loaded ratchets from disk", "count", len(enc), "path", s.path)
	} else if os.IsNotExist(err) {
		return ioutil.WriteFile(s.path, nil, 0644)
	}
	return nil
}

func (s *ratchetStore) Flush() error {
	if s.path != "" {
		log.Debugw("flushing ratchets", "path", s.path)
		enc := map[string]string{}
		for k, v := range s.cache {
			enc[k] = v.Encode()
		}
		d, err := json.Marshal(enc)
		if err != nil {
			return err
		}
		return ioutil.WriteFile(s.path, d, 0644)
	}
	return nil
}
