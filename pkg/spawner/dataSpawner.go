package spawner

import (
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/ironarachne/namegen"
)

type dataSpawner struct {
	ID              int32
	idLock          sync.Mutex
	ContinentSource []string
	NameGenerators  []namegen.NameGenerator
	Genders         []string
}

var nameAreas = []string{
	"anglosaxon",
	"dutch",
	"dwarf",
	"elf",
	"english",
	"estonian",
	"fantasy",
	"finnish",
	"french",
	"german",
	"greek",
	"hindu",
	"icelandic",
	"indonesian",
	"italian",
	"japanese",
	"korean",
	"mayan",
	"nepalese",
	"norwegian",
	"portuguese",
	"russian",
	"spanish",
	"swedish",
	"thai",
}

func NewDataSpawner() *dataSpawner {
	ngs := make([]namegen.NameGenerator, 0, len(nameAreas)*2)
	for _, a := range nameAreas {
		ngs = append(ngs,
			namegen.NameGeneratorFromType(a, `male`),
			namegen.NameGeneratorFromType(a, `female`),
		)
	}

	newSpawner := &dataSpawner{
		ID:              0,
		ContinentSource: []string{`North America`, `Asia`, `South America`, `Europe`, `Africa`, `Australia`},
		NameGenerators:  ngs,
		Genders:         []string{`male`, `female`},
	}
	return newSpawner
}

// ContinentGenerator
// one value from the following values {“North America”, “Asia”, “South America”, “Europe”, “Africa”, “Australia”}
func (sp *dataSpawner) ContinentGenerator() string {
	i := rand.Intn(len(sp.ContinentSource))
	return sp.ContinentSource[i]
}

// NameGenerator names are strings with the English character only, length ranging from 10-15
func (sp *dataSpawner) NameGenerator() string {
	i := rand.Intn(len(sp.ContinentSource))
	j := rand.Intn(2)
	for {
		name, e := sp.NameGenerators[i].CompleteName(sp.Genders[j])
		if e != nil {
			continue
		}
		if len(name) < 10 {
			name += ` your name is too short`
		}
		if len(name) > 15 {
			name = name[:15]
		}
		return name
	}
}

// IDGenerator integer number within 32-bit range
func (sp *dataSpawner) IDGenerator() int32 {
	sp.idLock.Lock()
	defer sp.idLock.Unlock()
	sp.ID += 1
	return sp.ID
}

var (
	numbersBook = []string{`0`, `1`, `2`, `3`, `4`, `5`, `6`, `7`, `8`, `9`}
	Alphabet    = []string{
		`a`, `b`, `c`, `d`, `e`, `f`, `g`,
		`h`, `i`, `j`, `k`, `l`, `m`,
		`o`, `p`, `q`, `r`, `s`, `t`,
		`u`, `v`, `w`, `x`, `y`, `z`,
	}
)

func GenNumWords() (res string) {
	_len := []int{4, 5, 6}
	l := _len[rand.Intn(len(_len))]
	for i := 0; i < l; i++ {
		res += numbersBook[rand.Intn(len(numbersBook))]
	}
	return res
}

func GenAlphabetWords() (res string) {
	_len := []int{4, 5, 6}
	l := _len[rand.Intn(len(_len))]
	for i := 0; i < l; i++ {
		a := Alphabet[rand.Intn(len(Alphabet))]
		upper := rand.Intn(2)
		if upper == 1 {
			a = strings.ToUpper(a)
		}
		res += a
	}

	return res
}

// AddressGenerator
// addresses are strings with a mixture of numbers, characters, and space, length ranging from 15-20
func (sp *dataSpawner) AddressGenerator() (addr string) {
	for {
		number := rand.Intn(5)
		if number == 0 {
			addr += ` ` + GenNumWords()
		} else {
			addr += ` ` + GenAlphabetWords()
		}
		if len(addr) >= 15 {
			break
		}
	}
	if len(addr) > 20 {
		addr = addr[:20]
	}
	return addr
}

func (sp *dataSpawner) Spawn() (line string) {
	lines := []string{
		strconv.Itoa(int(sp.IDGenerator())),
		sp.NameGenerator(),
		sp.AddressGenerator(),
		sp.ContinentGenerator(),
	}
	line = strings.Join(lines, `,`)
	return line
}
