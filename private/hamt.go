package private

import (
	"fmt"
	"io"

	cbg "github.com/whyrusleeping/cbor-gen"
)

// A CBOR-marshalable byte array.
type CborByteArray []byte

func (c *CborByteArray) MarshalCBOR(w io.Writer) error {
	if err := cbg.WriteMajorTypeHeader(w, cbg.MajByteString, uint64(len(*c))); err != nil {
		return err
	}
	_, err := w.Write(*c)
	return err
}

func (c *CborByteArray) UnmarshalCBOR(r io.Reader) error {
	maj, extra, err := cbg.CborReadHeader(r)
	if err != nil {
		return err
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}
	if uint64(cap(*c)) < extra {
		*c = make([]byte, extra)
	}
	if _, err := io.ReadFull(r, *c); err != nil {
		return err
	}
	return nil
}
