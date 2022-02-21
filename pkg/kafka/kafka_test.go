package kafka

import (
	"fmt"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"helloTigerGraph/pkg/db"
	"helloTigerGraph/pkg/model"
	"helloTigerGraph/pkg/spawner"
)

func TestInsert2(t *testing.T) {
	topic := `test`
	var million = 1000000
	_len := million * 5
	_len = 50000
	sp := spawner.NewDataSpawner()
	mx := 0
	tmp := make([]*model.Schema, 10000)
	bufed := false
	_db, e := db.NewDatabase()
	require.NoError(t, e)
	i := 0
	for {
		j := i % 10000
		line := sp.Spawn()
		if i == _len {
			if bufed {
				if j == 0 {
					j = 10000
				}
				log.Info().Msgf(`Insert *** %d`, i)
				err := db.Insert(_db, tmp[:j]...)
				require.NoError(t, err)
				err = Insert(topic, tmp[:j]...)
				require.NoError(t, err)
			}
			break
		}
		if j == 0 && i != 0 {
			log.Info().Msgf(`Insert *** %d`, i)
			err := db.Insert(_db, tmp...)
			require.NoError(t, err)
			err = Insert(topic, tmp...)
			require.NoError(t, err)
			// tmp = tmp
			bufed = false
		}
		scm, err := model.NewSchema(string(line))
		require.NoError(t, err)
		tmp[j] = scm
		bufed = true
		i++
	}
	spew.Dump(mx)
}

func TestInsert(t *testing.T) {
	s1 := &model.Schema{
		ID:        0,
		Name:      "weipeng",
		Address:   "xxxxxxxx",
		Continent: "ccc",
	}
	e := Insert(`test`, s1)
	require.NoError(t, e)
}

func ReadCSV(t *testing.T, topic string) {
	cx, e := Read(topic)
	require.NoError(t, e)
	f, e := os.Create(fmt.Sprintf(`%s.csv`, topic))
	require.NoError(t, e)
	defer f.Close()
	i := 0
LOOP:
	for {
		select {
		case scm, ok := <-cx:
			if i%10000 == 0 {
				log.Info().Msgf(`Read %s *** %d %v`, topic, i, ok)
			}
			if !ok {
				break LOOP
			}
			f.WriteString(scm.String() + "\n")
			i++
		}
	}
}

func TestCSV(t *testing.T) {
	ReadCSV(t, `id`)
	ReadCSV(t, `name`)
	ReadCSV(t, `continent`)
}
