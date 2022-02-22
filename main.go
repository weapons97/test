package main

import (
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

	"helloTigerGraph/pkg/db"
	"helloTigerGraph/pkg/kafka"
	"helloTigerGraph/pkg/model"
	"helloTigerGraph/pkg/spawner"
)

func CheckErr(e error) {
	if e == nil {
		return
	}
	log.Panic().Err(e).Send()
}

func ProcessSpawn(topic string, _len int) {
	sp := spawner.NewDataSpawner()
	lx := 1000
	tmp := make([]*model.Schema, lx)
	bufed := false
	_db, e := db.NewDatabase()
	CheckErr(e)
	i := 0
	for {
		j := i % lx
		line := sp.Spawn()
		if i == _len {
			if bufed {
				if j == 0 {
					j = lx
				}
				log.Info().Msgf(`Insert *** %d`, i)
				err := db.Insert(_db, tmp[:j]...)
				CheckErr(err)
				err = kafka.Insert(topic, tmp[:j]...)
				CheckErr(err)
			}
			break
		}
		if j == 0 && i != 0 {
			log.Info().Msgf(`Insert *** %d`, i)
			err := db.Insert(_db, tmp...)
			CheckErr(err)
			err = kafka.Insert(topic, tmp...)
			CheckErr(err)
			// tmp = tmp
			bufed = false
		}
		scm, err := model.NewSchema(string(line))
		CheckErr(err)
		tmp[j] = scm
		bufed = true
		i++
	}
}

func ProcessName() {
	_db, e := db.NewDatabase()
	CheckErr(e)
	cx, e := db.Search(_db, `name`)
	CheckErr(e)
	i := 0
	lx := 10000
	tmp := make([]*model.Schema, lx)
	bufed := false
LOOP:
	for {
		select {
		case scm, ok := <-cx:
			j := i % lx
			if !ok {
				if bufed {
					if j == 0 {
						j = lx
					}
					e = kafka.Insert(`name`, tmp[:j]...)
					CheckErr(e)
				}
				break LOOP
			}
			if j == 0 && i != 0 {
				log.Info().Msgf(`name *** %d %v`, i, ok)
				e = kafka.Insert(`name`, tmp...)
				CheckErr(e)
				bufed = false
			}
			tmp[j] = scm
			bufed = true
			i++
		}
	}
	return
}

func ProcessID() {
	_db, e := db.NewDatabase()
	CheckErr(e)
	cx, e := db.Search(_db, `id`)
	CheckErr(e)
	i := 0
	lx := 10000
	tmp := make([]*model.Schema, lx)
	bufed := false
LOOP:
	for {
		select {
		case scm, ok := <-cx:
			j := i % lx
			if !ok {
				if bufed {
					if j == 0 {
						j = lx
					}
					e = kafka.Insert(`id`, tmp[:j]...)
					CheckErr(e)
				}
				break LOOP
			}
			if j == 0 && i != 0 {
				log.Info().Msgf(`id *** %d %v`, i, ok)
				e = kafka.Insert(`id`, tmp...)
				CheckErr(e)
				bufed = false
			}
			tmp[j] = scm
			bufed = true
			i++
		}
	}
	return
}

//func ReadByID(id model.SchemaID, f *os.File) ([]byte, error) {
//	e := indexer.Seek(f, id)
//	if e != nil {
//		return nil, e
//	}
//	endpos := db.Id2filePositionRead(id + 1)
//	if endpos != 0 {
//		_len := int(endpos) - int(db.Id2filePositionRead(id))
//		buf := make([]byte, _len)
//		_, err := f.Read(buf)
//		return buf, err
//	}
//	return ioutil.ReadAll(f)
//}

func ProcessContinent() {
	_db, e := db.NewDatabase()
	CheckErr(e)
	cx, e := db.Search(_db, `continent`)
	CheckErr(e)
	i := 0
	lx := 10000
	tmp := make([]*model.Schema, lx)
	bufed := false
LOOP:
	for {
		select {
		case scm, ok := <-cx:
			j := i % lx
			if !ok {
				if bufed {
					if j == 0 {
						j = lx
					}
					e = kafka.Insert(`continent`, tmp[:j]...)
					CheckErr(e)
				}
				break LOOP
			}
			if j == 0 && i != 0 {
				log.Info().Msgf(`continent *** %d %v`, i, ok)
				e = kafka.Insert(`continent`, tmp...)
				CheckErr(e)
				bufed = false
			}
			tmp[j] = scm
			bufed = true
			i++
		}
	}
	return
}

func main() {
	var loop = 1000000 * 50
	if len(os.Args) > 1 {
		l, err := strconv.Atoi(os.Args[1])
		if err == nil {
			loop = l
		}
	}
	loop = loop

	log.Info().Msgf(`args %+v`, os.Args)
	s1 := time.Now()
	ProcessSpawn(`source`, loop)
	runtime.GC()
	ProcessID()
	runtime.GC()
	ProcessName()
	runtime.GC()
	ProcessContinent()
	s2 := time.Now()
	log.Info().Msgf(`total... in %s`, s2.Sub(s1))
}
