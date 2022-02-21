package db

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"helloTigerGraph/pkg/model"
	"helloTigerGraph/pkg/spawner"
)

func TestCsvFile(t *testing.T) {
	f, e := os.Create(`schema.csv`)
	require.NoError(t, e)
	defer f.Close()
	var million = 1000000
	var _len = million * 5
	_len = 50000
	sp := spawner.NewDataSpawner()
	for i := 0; i < _len; i++ {
		if i%10000 == 0 {
			log.Info().Msgf(`spawning *** %d`, i)
		}
		line := sp.Spawn()
		_, e := f.WriteString(line + "\n")
		require.NoError(t, e)
	}
}

func TestDBInsert(t *testing.T) {
	f, e := os.Open(`schema.csv`)
	require.NoError(t, e)
	defer f.Close()
	db, e := NewDatabase()
	require.NoError(t, e)
	rd := bufio.NewReader(f)
	i := 0
	tmp := make([]*model.Schema, 10000)
	bufed := false
	for {
		// j := i % 10000
		j := i % 10000
		line, _, err := rd.ReadLine()
		if errors.Is(err, io.EOF) {
			if bufed {
				if j == 0 {
					j = 10000
				}
				err = Insert(db, tmp[:j]...)
				require.NoError(t, err)
			}
			break
		}
		if j == 0 && i != 0 {
			log.Info().Msgf(`Insert *** %d`, i)
			err = Insert(db, tmp...)
			require.NoError(t, err)
			// tmp = tmp
			bufed = false
		}
		scm, err := model.NewSchema(string(line))
		tmp[j] = scm
		bufed = true
		i++
	}
	fmt.Println()
}

func TestDBByName(t *testing.T) {
	db, e := NewDatabase()
	require.NoError(t, e)
	cx, e := Search(db, `name`)
	require.NoError(t, e)
	f, e := os.Create(`byName.csv`)
	require.NoError(t, e)
	defer f.Close()
	i := 0
LOOP:
	for {
		select {
		case scm, ok := <-cx:
			if i%10000 == 0 {
				log.Info().Msgf(`byName *** %d %v`, i, ok)
			}
			if !ok {
				break LOOP
			}
			f.WriteString(scm.String() + "\n")
			i++
		}
	}
}
