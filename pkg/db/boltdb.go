package db

//
//import (
//	"github.com/boltdb/bolt"
//	"github.com/rs/zerolog/log"
//
//	"helloTigerGraph/pkg/model"
//)
//
//var bkName = `Id2filePosition`
//var globalDB = NewBoltDB()
//
//func NewBoltDB() (db *bolt.DB) {
//	db, err := bolt.Open("id2line.db", 0600, nil)
//	if err != nil {
//		log.Panic().Err(err).Send()
//	}
//	err = db.Update(func(tx *bolt.Tx) error {
//		b := tx.Bucket([]byte(bkName))
//		if b == nil {
//			_, err := tx.CreateBucket([]byte(bkName))
//			if err != nil {
//				log.Panic().Err(err).Send()
//			}
//		}
//		return nil
//	})
//	if err != nil {
//		log.Panic().Err(err).Send()
//	}
//	return db
//}
//
//func i32tob(val uint32) []byte {
//	r := make([]byte, 4)
//	for i := uint32(0); i < 4; i++ {
//		r[i] = byte((val >> (8 * i)) & 0xff)
//	}
//	return r
//}
//
//func btoi32(val []byte) uint32 {
//	if len(val) == 0 {
//		return 0
//	}
//	r := uint32(0)
//	for i := uint32(0); i < 4; i++ {
//		r |= uint32(val[i]) << (8 * i)
//	}
//	return r
//}
//
//type KV struct {
//	K, V uint32
//}
//
//// Id2filePosition
//func Id2filePositionInsert(kvs ...KV) {
//	err := globalDB.Update(func(tx *bolt.Tx) error {
//		b := tx.Bucket([]byte(bkName))
//		if b == nil {
//			log.Fatal().Msg(`bad bucket name`)
//		}
//		for _, kv := range kvs {
//			b.Put(i32tob(kv.K), i32tob(kv.V))
//		}
//		return nil
//	})
//	if err != nil {
//		log.Panic().Err(err).Send()
//	}
//}
//
//func Id2filePositionRead(k model.SchemaID) (res uint32) {
//	err := globalDB.View(func(tx *bolt.Tx) error {
//
//		b := tx.Bucket([]byte(bkName))
//		if b == nil {
//			log.Fatal().Msg(`bad bucket name`)
//		}
//		data := b.Get(i32tob(uint32(k)))
//		res = btoi32(data)
//		return nil
//	})
//	if err != nil {
//		log.Panic().Err(err).Send()
//	}
//	return res
//}
