package spawner

import (
	"log"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

func TestSpawner(t *testing.T) {
	sp := NewDataSpawner()
	l := sp.Spawn()
	spew.Dump(l)
	spew.Dump(len(sp.ContinentSource[2]))
}

func TestCsvFile(t *testing.T) {
	f, e := os.Create(`schema.csv`)
	require.NoError(t, e)
	defer f.Close()
	var million = 1000000
	sp := NewDataSpawner()
	mx := 0
	for i := 0; i < million*5; i++ {
		if i%1000 == 0 {
			log.Println(`spawning...`, i)
		}
		line := sp.Spawn()
		if len(line) > mx {
			mx = len(line)
		}
		_, e := f.WriteString(line + "\n")
		require.NoError(t, e)
	}
	spew.Dump(mx)
}
