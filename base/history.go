package base

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

func History(store mdstore.MerkleDagStore, n Node, max int) ([]HistoryEntry, error) {
	log := []HistoryEntry{
		n.AsHistoryEntry(),
	}

	prev := log[0].Previous
	for prev != nil {
		ent, err := LoadHistoryEntry(store, *prev)
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

func LoadHistoryEntry(store mdstore.MerkleDagStore, id cid.Cid) (HistoryEntry, error) {
	node, err := store.GetNode(id)
	if err != nil {
		return HistoryEntry{}, err
	}

	links := node.Links()
	ent := HistoryEntry{
		Cid:  id,
		Size: node.Size(),
	}
	if mdLnk := links.Get(MetadataLinkName); mdLnk != nil {
		ent.Metadata, err = LoadMetadata(store, mdLnk.Cid)
	}
	if prvLnk := links.Get(PreviousLinkName); prvLnk != nil {
		ent.Previous = &prvLnk.Cid
	}
	return ent, err
}
