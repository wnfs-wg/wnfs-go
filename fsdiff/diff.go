package fsdiff

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"

	golog "github.com/ipfs/go-log"
)

var log = golog.Logger("wnfs")

const (
	DTUnchanged DeltaType = iota
	DTAdd
	DTChange
	DTRemove
)

type DeltaType uint8

func (t DeltaType) String() string {
	switch t {
	case DTUnchanged:
		return " "
	case DTAdd:
		return "A"
	case DTChange:
		return "M"
	case DTRemove:
		return "R"
	default:
		return "?"
	}
}

type Delta struct {
	Type   DeltaType
	Name   string
	Deltas []*Delta
}

func (d Delta) String() string {
	return fmt.Sprintf("%s %s", d.Type, d.Name)
}

func (d Delta) Changed() bool {
	return d.Type != DTUnchanged
}

func Tree(aPath, bPath string, afs, bfs fs.FS, ignore ...string) (*Delta, error) {
	changes := &Delta{Type: DTUnchanged, Name: "."}
	ignoreMap := map[string]struct{}{}
	for _, ig := range ignore {
		ignoreMap[ig] = struct{}{}
	}
	err := tree(aPath, bPath, afs, bfs, changes, ignoreMap)
	log.Debugw("Tree", "changes", changes)
	return changes, err
}

func tree(aPath, bPath string, afs, bfs fs.FS, changes *Delta, ignore map[string]struct{}) error {
	a, err := afs.Open(aPath)
	if err != nil {
		return err
	}

	b, err := bfs.Open(bPath)
	if err != nil {
		return err
	}

	ai, err := a.Stat()
	if err != nil {
		return err
	}
	bi, err := b.Stat()
	if err != nil {
		return err
	}

	// handle root file / directory mismatch: drop a, add b
	if (ai.IsDir() && !bi.IsDir()) || (!ai.IsDir() && bi.IsDir()) {
		log.Debugf("a %s: isDir: %t, b %s: isDir: %t", ai.Name(), ai.IsDir(), bi.Name(), bi.IsDir())
		changes.Type = DTChange
		changes.Deltas = append(changes.Deltas,
			&Delta{Type: DTRemove, Name: ai.Name()},
			&Delta{Type: DTAdd, Name: bi.Name()},
		)
		return nil
	} else if !ai.IsDir() && !bi.IsDir() {
		// both a & b are files
		if ai.Name() != bi.Name() {
			changes.Type = DTChange
			// represent name change as an add/remove delta
			changes.Deltas = append(changes.Deltas,
				&Delta{Type: DTRemove, Name: ai.Name()},
				&Delta{Type: DTAdd, Name: bi.Name()},
			)
			return nil
		}

		// TODO(b5): checking file stats first will be much faster, but requires
		// syncing all filesystem metadata, should be optional & opt-in
		// if !ai.ModTime().Equal(bi.ModTime()) ||
		// 	ai.Mode() != bi.Mode() ||
		// 	ai.Size() != bi.Size() {
		// 	// mark any non-name stats change as a modification
		// 	changes.Deltas = append(changes.Deltas,
		// 		Delta{Type: DTChange, Name: ai.Name()},
		// 	)
		// }

		// check if file contents are identitical. reads all contents of both files
		if eq, err := readersEqual(a, b); err != nil {
			return err
		} else if !eq {
			changes.Type = DTChange
		}

		return nil
	}

	// both a & b must be directory files
	return diffDirectoryFiles(aPath, bPath, a, b, afs, bfs, changes, ignore)
}

func diffDirectoryFiles(aPath, bPath string, a, b fs.File, afs, bfs fs.FS, changes *Delta, ignore map[string]struct{}) error {
	aDir, ok := a.(fs.ReadDirFile)
	if !ok {
		return errors.New("cannot access contents of directory file")
	}
	bDir, ok := b.(fs.ReadDirFile)
	if !ok {
		return errors.New("cannot access contents of directory file")
	}

	aFiles, err := aDir.ReadDir(-1)
	if err != nil {
		return err
	}
	bFiles, err := bDir.ReadDir(-1)
	if err != nil {
		return err
	}

	// calculate the set intersection between aFiles & bFiles, assumes filenames
	// are sorted and all filenames are unique
	aFilesMap := make(map[string]fs.DirEntry, len(aFiles))
	for _, f := range aFiles {
		if _, ok := ignore[f.Name()]; ok {
			continue
		}
		aFilesMap[f.Name()] = f
	}

	for _, bFile := range bFiles {
		name := bFile.Name()
		if _, ok := ignore[name]; ok {
			continue
		}

		if _, foundInA := aFilesMap[name]; !foundInA {
			// file is missing in a, exists in b, mark as added
			changes.Type = DTChange
			changes.Deltas = append(changes.Deltas,
				&Delta{Type: DTAdd, Name: name},
			)
			continue
		}

		// file exists in both b & a
		aChPath := filepath.Join(aPath, name)
		bChPath := filepath.Join(bPath, name)
		childChanges := &Delta{Name: name}
		// recurse
		if err := tree(aChPath, bChPath, afs, bfs, childChanges, ignore); err != nil {
			return err
		}
		if childChanges.Changed() {
			changes.Type = DTChange
		}
		changes.Deltas = append(changes.Deltas, childChanges)

		// remove matched file from a files map
		delete(aFilesMap, name)
	}

	if len(aFilesMap) > 0 {
		changes.Type = DTChange
		// everything left in the common map is a removal
		for name := range aFilesMap {
			changes.Deltas = append(changes.Deltas,
				&Delta{Type: DTRemove, Name: name},
			)
		}
	}

	return nil
}

func readComplete(r io.Reader, b []byte) (int, error) {
	var (
		n   int
		err error
	)
	for _n := 0; n < len(b) && err == nil; n += _n {
		_n, err = r.Read(b[n:])
	}
	return n, err
}

// readersEqual compares the contents of two io.Readers.
// The return value of identical is true if and only if there are no errors
// in reading r1 and r2 (io.EOF excluded) and r1 and r2 are
// byte-for-byte identical.
//
// pulled from https://play.golang.org/p/g-uXGjNr-6
// discussion: https://groups.google.com/g/golang-nuts/c/keG78hYt1I0
func readersEqual(r1, r2 io.Reader) (identical bool, err error) {
	const size = 4096
	var (
		input = [...]io.Reader{r1, r2}

		n    [2]int
		errs [2]error
		buf  [2][size]byte
	)
	for {
		for i, r := range input {
			n[i], errs[i] = readComplete(r, buf[i][:])
			if errs[i] != nil && errs[i] != io.EOF {
				return false, errs[i]
			}
		}
		if errs[0] == io.EOF || errs[1] == io.EOF {
			for i, err := range errs {
				if err == nil {
					_, errs[i] = input[i].Read([]byte{0})
				}
			}
			return errs[0] == errs[1] && bytes.Equal(buf[0][:n[0]], buf[1][:n[1]]), nil
		}
		if !bytes.Equal(buf[0][:n[0]], buf[1][:n[1]]) {
			return false, nil
		}
	}
}
