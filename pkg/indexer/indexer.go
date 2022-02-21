package indexer

import (
	"os"

	"github.com/rs/zerolog/log"

	"helloTigerGraph/pkg/db"
	"helloTigerGraph/pkg/model"
)

type Schema = model.Schema
type IDIdexer []*Schema

func (idx IDIdexer) Len() int           { return len(idx) }
func (idx IDIdexer) Less(i, j int) bool { return idx[i].ID < idx[j].ID }
func (idx IDIdexer) Swap(i, j int)      { idx[i], idx[j] = idx[j], idx[i] }

func (idx *IDIdexer) Push(t interface{}) {
	sma := t.(*Schema)
	*idx = append(*idx, sma)
}

func (idx *IDIdexer) Pop() interface{} {
	old := *idx
	n := len(old)
	x := old[n-1]
	*idx = old[0 : n-1]
	return x
}

type NameID struct {
	ID   model.SchemaID
	Name string
}
type NameIdexer []NameID

func (idx NameIdexer) Len() int           { return len(idx) }
func (idx NameIdexer) Less(i, j int) bool { return idx[i].Name < idx[j].Name }
func (idx NameIdexer) Swap(i, j int)      { idx[i], idx[j] = idx[j], idx[i] }

func (idx *NameIdexer) Push(t interface{}) {
	sma := t.(*Schema)
	val := NameID{
		ID:   sma.ID,
		Name: sma.Name,
	}
	*idx = append(*idx, val)
}

func (idx *NameIdexer) Pop() interface{} {
	old := *idx
	n := len(old)
	x := old[n-1]
	*idx = old[0 : n-1]
	return x
}

type ContinentIdexer struct {
	Index  map[string][]model.SchemaID
	Sorted []string
}

func NewContinentIdexer() *ContinentIdexer {
	rx := new(ContinentIdexer)
	sorted := []string{`Africa`, `Asia`, `Australia`, `Europe`, `North America`, `South America`}
	rx.Index = make(map[string][]model.SchemaID)
	for _, continent := range sorted {
		rx.Index[continent] = make([]model.SchemaID, 0, 100000)
	}
	rx.Sorted = sorted
	return rx
}

func Seek(file *os.File, id model.SchemaID) error {
	pos := db.Id2filePositionRead(id)
	ret, e := file.Seek(int64(pos), os.SEEK_SET)
	if e != nil {
		log.Error().Int64(`ret`, ret).Msgf(`getline seek failed %+v`, e)
		return e
	}
	return e
}
