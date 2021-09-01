package private

import (
	"encoding/base64"
	"encoding/json"
)

type Key [32]byte

func NewKey() Key {
	return NewSpiralRatchet().Key()
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

func (k Key) IsEmpty() bool { return k == Key([32]byte{}) }

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
