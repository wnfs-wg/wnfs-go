package wnfs

import (
	"encoding/base64"
	"encoding/json"
)

type Key [32]byte

func NewKey() Key {
	return NewSpiralRatchet().Key()
}

func (k Key) IsEmpty() bool { return k == Key([32]byte{}) }

func (k Key) MarshalJSON() ([]byte, error) {
	return []byte(`"` + base64.URLEncoding.EncodeToString(k[:]) + `"`), nil
}

func (k *Key) UnmarshalJSON(d []byte) error {
	var s string
	if err := json.Unmarshal(d, &s); err != nil {
		return err
	}
	data, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	for i, d := range data {
		k[i] = d
	}
	return nil
}
