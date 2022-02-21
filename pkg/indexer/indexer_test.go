package indexer

import (
	"bufio"
	"container/heap"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"helloTigerGraph/pkg/db"
	"helloTigerGraph/pkg/model"
)

func TestIndexer(t *testing.T) {
	idIDX := IDIdexer{}
	nameIDX := NameIdexer{}
	f, e := os.Open(`schema.csv`)
	require.NoError(t, e)
	defer f.Close()
	heap.Init(&idIDX)
	heap.Init(&nameIDX)
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		scm, err := model.NewSchema(string(line))
		require.NoError(t, err)
		heap.Push(&idIDX, scm)
		heap.Push(&nameIDX, scm)
	}
	f2, e := os.Create(`name2id.csv`)
	require.NoError(t, e)
	defer f2.Close()
	for _, s := range nameIDX {
		f2.WriteString(s.Name + `,` + strconv.Itoa(int(s.ID)) + "\n")
	}

}

func TestNameIDX(t *testing.T) {
	nameIDX := NameIdexer{}
	f, e := os.Open(`schema.csv`)
	require.NoError(t, e)
	defer f.Close()
	// heap.Init(&nameIDX)
	rd := bufio.NewReader(f)
	var seek uint32
	for {
		line, err := rd.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		scm, err := model.NewSchema(string(line))
		require.NoError(t, err)
		db.Id2filePositionInsert(db.KV{K: uint32(scm.ID), V: seek})
		seek += uint32(len(line))
		val := NameID{
			ID:   scm.ID,
			Name: scm.Name,
		}
		nameIDX = append(nameIDX, val)
		// heap.Push(&nameIDX, scm)
	}

	sort.Sort(nameIDX)
	//e = getLine(f, 0, 43)
	//require.NoError(t, e)
	//line, _, err := rd.ReadLine()
	//require.NoError(t, err)
	//fmt.Println(`???`, string(line))
	spew.Dump(len(nameIDX))
	f2, e := os.Create(`name.csv`)
	require.NoError(t, e)
	defer f2.Close()

	for i, s := range nameIDX {
		// f2.WriteString(s.Name + `,` + strconv.Itoa(int(s.ID)) + "\n")
		if i%10000 == 0 {
			log.Info().Msgf(`*** %d`, i)
		}
		e := Seek(f, s.ID)
		require.NoError(t, e)
		_len := 1024
		endpos := db.Id2filePositionRead(s.ID + 1)
		if endpos != 0 {
			_len = int(endpos) - int(db.Id2filePositionRead(s.ID))
		}

		buf := make([]byte, _len)
		_, err := f.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			require.NoError(t, err)
		}
		//line := strings.TrimSpace(string(buf))
		n, e := f2.Write(buf)
		require.NoError(t, e)
		require.Equal(t, n, len(buf))
	}
}

func TestContinent(t *testing.T) {
	ci := NewContinentIdexer()
	f, e := os.Open(`schema.csv`)
	require.NoError(t, e)
	defer f.Close()
	// heap.Init(&nameIDX)
	rd := bufio.NewReader(f)
	var seek uint32
	for {
		line, err := rd.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		scm, err := model.NewSchema(string(line))
		require.NoError(t, err)

		db.Id2filePositionInsert(db.KV{K: uint32(scm.ID), V: seek})
		seek += uint32(len(line))
		ci.Index[scm.Continent] = append(ci.Index[scm.Continent], scm.ID)
	}
	f2, e := os.Create(`continent.csv`)
	require.NoError(t, e)
	defer f2.Close()
	i := 0
	for _, s := range ci.Sorted {
		for _, id := range ci.Index[s] {
			if i%10000 == 0 {
				log.Info().Msgf(`continent *** %d`, i)
			}
			i++
			e := Seek(f, id)
			require.NoError(t, e)
			var buf []byte
			endpos := db.Id2filePositionRead(id + 1)
			if endpos != 0 {
				_len := int(endpos) - int(db.Id2filePositionRead(id))
				buf := make([]byte, _len)
				_, err := f.Read(buf)
				if err != nil && !errors.Is(err, io.EOF) {
					require.NoError(t, err)
				}
			} else {
				buf, e = ioutil.ReadAll(f)
				if e != nil && !errors.Is(e, io.EOF) {
					require.NoError(t, e)
				}
			}

			n, e := f2.Write(buf)
			require.NoError(t, e)
			require.Equal(t, n, len(buf))
		}

	}
}
