package wnfs

import (
	"fmt"
	"sync"

	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

var nibbles = map[byte]struct{}{
	'0': {}, '1': {}, '2': {}, '3': {},
	'4': {}, '5': {}, '6': {}, '7': {},
	'8': {}, '9': {}, 'a': {}, 'b': {},
	'c': {}, 'd': {}, 'e': {}, 'f': {},
}

func isNibble(r byte) bool {
	_, exists := nibbles[r]
	return exists
}

// MMPT is a Modified Merkle Patricia Tree, with a node weight of 16
// It stores items with hexidecimal keys and creates a new layer when a given
// layer has two keys that start with the same nibble
type MMPT struct {
	fs    mdstore.MerkleDagStore
	id    cid.Cid
	links mdstore.Links
	size  int64

	lk       sync.Mutex
	children map[string]*MMPT
}

var _ mdstore.DagNode = (*MMPT)(nil)

func NewMMPT(fs mdstore.MerkleDagStore, links mdstore.Links) *MMPT {
	return &MMPT{
		fs:       fs,
		links:    links,
		children: map[string]*MMPT{},
	}
}

func LoadMMPT(fs mdstore.MerkleDagStore, id cid.Cid) (*MMPT, error) {
	log.Debugw("LoadMMPT", "cid", id)
	res, err := fs.GetNode(id)
	if err != nil {
		return nil, err
	}

	return &MMPT{
		fs:       fs,
		id:       id,
		size:     res.Size(),
		links:    res.Links(),
		children: map[string]*MMPT{},
	}, nil
}

func (t *MMPT) Cid() cid.Cid         { return t.id }
func (t *MMPT) Size() int64          { return t.size }
func (t *MMPT) Links() mdstore.Links { return t.links }

func (t *MMPT) Put() (PutResult, error) {
	res, err := t.fs.PutNode(t.links)
	if err != nil {
		return PutResult{}, err
	}
	log.Debugw("Put", "linkLen", t.links.Len(), "res.Cid", res.Cid, "err", err)

	t.id = res.Cid
	return PutResult{
		Cid:  res.Cid,
		Size: res.Size,
	}, nil
}

func (t *MMPT) Add(name string, id cid.Cid) error {
	log.Debugw("Add", "name", name, "cid", id, "links", t.links.Map())
	if !isNibble(name[0]) {
		return fmt.Errorf("not a valid name, must be hexadecimal")
	}

	nextNameOrSib := t.nextTreeOrSiblingName(name)

	if nextNameOrSib == "" {
		// if no children starting with first char of name, then add with entire name as key
		t.links.Add(mdstore.Link{
			Name:   name,
			Cid:    id,
			IsFile: true,
		})
		_, err := t.Put()
		return err
	}

	if name == nextNameOrSib {
		// name already exists, skip.
		return nil
	}

	// if multiple children with first char of names, then add to that tree
	if len(nextNameOrSib) == 1 {
		nextTree, err := t.getDirectChild(nextNameOrSib)
		if err != nil {
			return err
		}
		if err := nextTree.Add(name[1:], id); err != nil {
			return err
		}
		return t.PutAndUpdateChildLink(nextNameOrSib)
	}

	// if one other child with first char of name, then put both into a child tree
	newTree := t.addEmptyChild(string(name[0]))
	nextCID := t.links.Get(nextNameOrSib).Cid
	t.removeChild(nextNameOrSib)
	if err := newTree.Add(name[1:], id); err != nil {
		return err
	}
	if err := newTree.Add(nextNameOrSib[1:], nextCID); err != nil {
		return err
	}
	return t.PutAndUpdateChildLink(string(name[0]))
}

func (t *MMPT) PutAndUpdateChildLink(name string) error {
	t.lk.Lock()
	defer t.lk.Unlock()

	res, err := t.children[name].Put()
	if err != nil {
		return err
	}

	t.links.Add(mdstore.Link{
		Name:   name,
		Cid:    res.Cid,
		IsFile: false,
		Size:   res.Size,
	})
	_, err = t.Put()
	return err
}

type Member struct {
	Name string
	Cid  cid.Cid
}

func fromLink(l mdstore.Link) Member {
	return Member{
		Name: l.Name,
		Cid:  l.Cid,
	}
}

func (t *MMPT) Members() (ms []Member, err error) {
	for name, lnk := range t.links.Map() {
		if len(name) > 1 {
			ms = append(ms, fromLink(lnk))
			continue
		}

		child, err := LoadMMPT(t.fs, lnk.Cid)
		if err != nil {
			return nil, fmt.Errorf("loading cid %s: %w", lnk.Cid, err)
		}
		childMembers, err := child.Members()
		if err != nil {
			return nil, err
		}
		for _, childMember := range childMembers {
			ms = append(ms, Member{
				Name: name + childMember.Name,
				Cid:  childMember.Cid,
			})
		}
	}

	return ms, nil
}

func (t *MMPT) Get(name string) (cid.Cid, error) {
	nextName := t.nextTreeName(name)
	if nextName == nil {
		return cid.Cid{}, ErrNotFound
	}

	if len(*nextName) > 1 {
		return t.links.Get(*nextName).Cid, nil
	}

	nextTree, err := t.getDirectChild(name)
	if err != nil {
		return cid.Cid{}, err
	}
	return nextTree.Get(name[1:])
}

func (t *MMPT) Exists(name string) (bool, error) {
	res, err := t.Get(name)
	if err != nil {
		return false, err
	}
	return res.Equals(cid.Cid{}), nil
}

func (t *MMPT) addEmptyChild(name string) *MMPT {
	tree := NewMMPT(t.fs, mdstore.NewLinks())
	t.children[name] = tree
	return tree
}

func (t *MMPT) getDirectChild(name string) (*MMPT, error) {
	if t.children[name] != nil {
		return t.children[name], nil
	}

	lnk := t.links.Get(name)
	if lnk == nil {
		return nil, ErrNotFound
	}

	child, err := LoadMMPT(t.fs, lnk.Cid)
	if err != nil {
		return nil, err
	}

	// check that the child wasn't added while retrieving the mmpt from the network
	if t.children[name] != nil {
		return t.children[name], nil
	}

	t.children[name] = child
	return child, nil
}

func (t *MMPT) nextTreeOrSiblingName(name string) string {
	nibble := name[0]
	if t.directChildExists(string(nibble)) {
		return string(nibble)
	}
	for k := range t.links.Map() {
		if k[0] == nibble {
			return k
		}
	}
	return ""
}

func (t *MMPT) directChildExists(name string) bool {
	return t.links.Get(name) != nil || t.children[name] != nil
}

func (t *MMPT) removeChild(name string) {
	t.links.Remove(name)
	delete(t.children, name)
}

func (t *MMPT) nextTreeName(name string) *string {
	nibble := string(name[0])
	if t.directChildExists(nibble) {
		return &nibble
	} else if t.directChildExists(name) {
		return &name
	}
	return nil
}
