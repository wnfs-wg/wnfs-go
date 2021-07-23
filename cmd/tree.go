package main

import (
	"errors"
	"io/fs"
	"path/filepath"

	"github.com/xlab/treeprint"
)

func treeString(fsys fs.FS, path string) (string, error) {
	f, err := fsys.Open(path)
	if err != nil {
		return "", err
	}

	tree, err := fileTree(fsys, path, f)
	if err != nil {
		return "", err
	}
	// to add a custom root name use `treeprint.NewWithRoot()` instead
	p := treeprint.New()
	if err := walkAdd(p, tree); err != nil {
		return "", err
	}

	return p.String(), nil
}

func walkAdd(p treeprint.Tree, v interface{}) error {
	switch x := v.(type) {
	case string:
		p.AddNode(x)
	case []interface{}:
		for _, v := range x {
			if err := walkAdd(p, v); err != nil {
				return err
			}
		}
	case map[string]interface{}:
		for dirName, dir := range x {
			br := p.AddBranch(dirName)
			sl, ok := dir.([]interface{})
			if !ok {
				return errors.New("treeRoot value should be of type []interface{}")
			}

			if err := walkAdd(br, sl); err != nil {
				return err
			}
		}
	}

	return nil
}

func fileTree(fsys fs.FS, path string, f fs.File) (interface{}, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	readDirFile, ok := f.(fs.ReadDirFile)
	if !ok {
		// if it's a file return a rooted single file name
		return fi.Name(), nil
	}

	dir, error := readDirFile.ReadDir(-1)
	if error != nil {
		return nil, error
	}
	tree := make([]interface{}, 0, len(dir))
	for _, ent := range dir {
		name := ent.Name()
		if ent.IsDir() {
			subPath := filepath.Join(path, name)
			f, err := fsys.Open(subPath)
			if err != nil {
				return nil, err
			}

			// recurse
			subTree, err := fileTree(fsys, subPath, f)
			if err != nil {
				return nil, err
			}

			tree = append(tree, subTree)
			continue
		}

		tree = append(tree, name)
	}

	return map[string]interface{}{
		fi.Name(): tree,
	}, nil
}
