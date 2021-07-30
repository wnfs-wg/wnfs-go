package wnfs

import (
	cid "github.com/ipfs/go-cid"
	"github.com/qri-io/wnfs-go/mdstore"
)

type HistoryEntry struct {
	Cid      cid.Cid   `json:"cid"`
	Previous *cid.Cid  `json:"previous"`
	Metadata *Metadata `json:"metadata"`
	Size     int64     `json:"size"`
}

func history(store mdstore.MerkleDagStore, n Node, max int) ([]HistoryEntry, error) {
	log := []HistoryEntry{
		n.AsHistoryEntry(),
	}

	prev := log[0].Previous
	for prev != nil {
		ent, err := loadHistoryEntry(store, *prev)
		if err != nil {
			return nil, err
		}
		log = append(log, ent)
		prev = ent.Previous

		if len(log) == max {
			break
		}
	}

	return log, nil
}

func loadHistoryEntry(store mdstore.MerkleDagStore, id cid.Cid) (HistoryEntry, error) {
	node, err := store.GetNode(id)
	if err != nil {
		return HistoryEntry{}, err
	}

	links := node.Links()
	ent := HistoryEntry{
		Cid:  id,
		Size: node.Size(),
	}
	if mdLnk := links.Get(metadataLinkName); mdLnk != nil {
		ent.Metadata, err = loadMetadata(store, mdLnk.Cid)
	}
	if prvLnk := links.Get(previousLinkName); prvLnk != nil {
		ent.Previous = &prvLnk.Cid
	}
	return ent, err
}
