package private

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/base"
	ratchet "github.com/qri-io/wnfs-go/private/ratchet"
)

var EmptyKey = Key([32]byte{})

type Key [32]byte

func NewKey() Key {
	return ratchet.NewSpiral().Key()
}

func (k Key) Encode() string { return base64.URLEncoding.EncodeToString(k[:]) }

func (k *Key) Decode(s string) error {
	data, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	for i, d := range data {
		k[i] = d
	}
	return nil
}

func (k Key) IsEmpty() bool { return k == EmptyKey }

func (k Key) MarshalJSON() ([]byte, error) {
	return []byte(`"` + k.Encode() + `"`), nil
}

func (k *Key) UnmarshalJSON(d []byte) error {
	var s string
	if err := json.Unmarshal(d, &s); err != nil {
		return err
	}
	return k.Decode(s)
}

type DecryptionStore interface {
	DecryptionFields(id cid.Cid) (sname Name, key Key, err error)
}

type WritableDecryptionStore interface {
	DecryptionStore
	PutDecryptionFields(id cid.Cid, sname Name, key Key) error
}

func NewDecryptionStore(filepath string) (WritableDecryptionStore, error) {
	s := &decryptionStore{
		path: filepath,
	}
	return s, s.load()
}

type decryptionStore struct {
	path string
	sync.Mutex
	cache map[cid.Cid]decryption
}

type decryption struct {
	Name
	Key
}

var _ WritableDecryptionStore = (*decryptionStore)(nil)

func (s *decryptionStore) PutDecryptionFields(id cid.Cid, name Name, key Key) error {
	s.Lock()
	defer s.Unlock()

	s.cache[id] = decryption{name, key}
	return s.write()
}

func (s *decryptionStore) DecryptionFields(id cid.Cid) (name Name, key Key, err error) {
	s.Lock()
	defer s.Unlock()

	if dec, ok := s.cache[id]; ok {
		return dec.Name, dec.Key, nil
	}

	return name, key, base.ErrNotFound
}

func (s *decryptionStore) load() error {
	s.cache = map[cid.Cid]decryption{}

	data, err := ioutil.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	strs := map[string]map[string]string{}
	if err := json.Unmarshal(data, &strs); err != nil {
		return err
	}

	for idstr, dec := range strs {
		id, err := cid.Parse(idstr)
		if err != nil {
			return fmt.Errorf("parsing cid %s: %w", idstr, err)
		}
		keystr := dec["key"]
		k := &Key{}
		if err = k.Decode(keystr); err != nil {
			return fmt.Errorf("parsing key %s: %w", keystr, err)
		}
		s.cache[id] = decryption{
			Name: Name(dec["name"]),
			Key:  *k,
		}
	}

	return nil
}

func (s *decryptionStore) write() error {
	asStrings := map[string]interface{}{}
	for id, dec := range s.cache {
		asStrings[id.String()] = map[string]interface{}{
			"name": string(dec.Name),
			"key":  dec.Key.Encode(),
		}
	}
	data, err := json.Marshal(asStrings)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(s.path, data, 0644)
}
