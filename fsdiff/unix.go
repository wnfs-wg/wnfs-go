package fsdiff

import (
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/sergi/go-diff/diffmatchpatch"
)

const MaxFileSize = 1024 * 10 // max diff size of 10MB

var ErrFileTooLarge = errors.New("file too big to diff")

type FileDiff struct {
	Type    DeltaType
	Path    string
	DiffErr string
	Diff    []diffmatchpatch.Diff
}

func Unix(aPath, bPath string, afs, bfs fs.FS, ignore ...string) (diffs []FileDiff, err error) {
	dmp := diffmatchpatch.New()
	tree, err := Tree(aPath, bPath, afs, bfs, ignore...)
	if err != nil {
		return nil, err
	}

	err = walkModified(aPath, tree, func(path string, delta *Delta) error {
		switch delta.Type {
		case DTAdd:
			bStr, err := fileString(path, bfs)
			if err != nil {
				if errors.Is(err, ErrFileTooLarge) {
					diffs = append(diffs, FileDiff{
						Type:    DTAdd,
						Path:    path,
						DiffErr: "file too large",
					})
					return nil
				}
				return err
			}

			diffs = append(diffs, FileDiff{
				Type: DTAdd,
				Path: path,
				Diff: dmp.DiffMain("", bStr, true),
			})
		case DTRemove:
			aStr, err := fileString(path, afs)
			if err != nil {
				if errors.Is(err, ErrFileTooLarge) {
					diffs = append(diffs, FileDiff{
						Type:    DTRemove,
						Path:    path,
						DiffErr: "file too large",
					})
					return nil
				}
				return err
			}

			diffs = append(diffs, FileDiff{
				Type: DTRemove,
				Path: path,
				Diff: dmp.DiffMain(aStr, "", true),
			})
		case DTChange:
			aStr, err := fileString(path, afs)
			if err != nil {
				if errors.Is(err, ErrFileTooLarge) {
					diffs = append(diffs, FileDiff{
						Type:    DTChange,
						Path:    path,
						DiffErr: "file too large",
					})
					return nil
				}
				return err
			}

			bStr, err := fileString(path, bfs)
			if err != nil {
				if errors.Is(err, ErrFileTooLarge) {
					diffs = append(diffs, FileDiff{
						Type:    DTChange,
						Path:    path,
						DiffErr: "file too large",
					})
					return nil
				}
				return err
			}

			diffs = append(diffs, FileDiff{
				Type: DTChange,
				Path: path,
				Diff: dmp.DiffMain(aStr, bStr, true),
			})
		}
		return nil
	})

	return diffs, err
}

func PrettyPrintFileDiffs(diffs []FileDiff) string {
	b := &strings.Builder{}
	dmp := diffmatchpatch.New()
	for _, f := range diffs {
		b.WriteString(fmt.Sprintf("%s %s\n", f.Type, f.Path))
		b.WriteString(dmp.DiffPrettyText(f.Diff))
	}

	return b.String()
}

func walkModified(path string, tree *Delta, visit func(dir string, delta *Delta) error) error {
	if len(tree.Deltas) == 0 && tree.Type != DTUnchanged {
		return visit(path, tree)
	}
	for _, ch := range tree.Deltas {
		if err := walkModified(filepath.Join(path, ch.Name), ch, visit); err != nil {
			return err
		}
	}

	return nil
}

func fileString(path string, fsys fs.FS) (string, error) {
	aFile, err := fsys.Open(path)
	if err != nil {
		return "", err
	}
	aStat, err := aFile.Stat()
	if err != nil {
		return "", err
	}
	if aStat.IsDir() {
		return "", nil
	}
	if aStat.Size() > MaxFileSize {
		return "", ErrFileTooLarge
	}
	data, err := ioutil.ReadAll(aFile)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
