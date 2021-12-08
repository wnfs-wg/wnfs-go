package ratchet

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestMemStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewMemStore(ctx)

	a := NewSpiral()
	updated, err := s.PutRatchet(ctx, "a", a)
	require.Nil(t, err)
	assert.True(t, updated)

	b := NewSpiral()
	updated, err = s.PutRatchet(ctx, "a", b)
	require.Nil(t, err)
	assert.False(t, updated)

	got, err := s.OldestKnownRatchet(ctx, "a")
	require.Nil(t, err)
	assert.Equal(t, a, got)

	got, err = s.OldestKnownRatchet(ctx, "unknown")
	assert.ErrorIs(t, err, ErrRatchetNotFound)
	assert.Nil(t, got)

	err = s.Flush()
	require.Nil(t, err)
}

func TestFileStore(t *testing.T) {
	path, err := ioutil.TempDir("", "wnfs_ratchet_test")
	require.Nil(t, err)
	defer os.Remove(path)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := NewStore(ctx, filepath.Join(path, "ratchets.json"))
	require.Nil(t, err)

	a := NewSpiral()
	updated, err := s.PutRatchet(ctx, "a", a)
	require.Nil(t, err)
	assert.True(t, updated)

	b := NewSpiral()
	updated, err = s.PutRatchet(ctx, "a", b)
	require.Nil(t, err)
	assert.False(t, updated)

	got, err := s.OldestKnownRatchet(ctx, "a")
	require.Nil(t, err)
	assert.Equal(t, a, got)

	got, err = s.OldestKnownRatchet(ctx, "unknown")
	assert.ErrorIs(t, err, ErrRatchetNotFound)
	assert.Nil(t, got)

	err = s.Flush()
	require.Nil(t, err)

	// creating a new store should load from file
	s, err = NewStore(ctx, filepath.Join(path, "ratchets.json"))
	require.Nil(t, err)
	got, err = s.OldestKnownRatchet(ctx, "a")
	require.Nil(t, err)
	assert.Equal(t, a, got)

	got, err = s.OldestKnownRatchet(ctx, "unknown")
	assert.ErrorIs(t, err, ErrRatchetNotFound)
	assert.Nil(t, got)
}
