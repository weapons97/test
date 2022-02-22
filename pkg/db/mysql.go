package db

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	"helloTigerGraph/pkg/model"
)

func NewDatabase() (*sql.DB, error) {
	dbStr := "root:test@tcp(127.0.0.1:3306)/test"
	// dbStr = "root:test@tcp(106.75.106.139:3306)/test"
	db, err := sql.Open(
		"mysql",
		dbStr)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(0)
	return db, nil
}

func Search(by string) (chan *model.Schema, error) {
	db, e := NewDatabase()
	if e != nil {
		log.Error().Err(e).Send()
		return nil, e
	}
	// 通过切片存储
	rows, e := db.Query("SELECT * FROM users order by " + by)
	if e != nil {
		log.Error().Err(e).Send()
		return nil, e
	}
	// 遍历
	c := make(chan *model.Schema, 10000)

	go func() {
		for rows.Next() {
			var scm model.Schema
			e := rows.Scan(&scm.ID, &scm.Name, &scm.Address, &scm.Continent)
			if e != nil {
				log.Error().Err(e).Send()
				close(c)
			}
			c <- &scm
		}
		log.Info().Msg(`search end`)
		close(c)
		db.Close()
	}()
	return c, nil
}

func Insert(scms ...*model.Schema) error {
	// 插入数据
	db, e := NewDatabase()
	if e != nil {
		log.Error().Err(e).Send()
		return e
	}
	if len(scms) == 0 {
		return nil
	}
	sqlStr := "INSERT INTO users(id, name, address, continent) VALUES "
	vals := []interface{}{}

	for _, row := range scms {
		sqlStr += "(?, ?, ?, ?),"
		vals = append(vals, row.ID, row.Name, row.Address, row.Continent)
	}
	//trim the last ,
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	//prepare the statement
	stmt, e := db.Prepare(sqlStr)
	if e != nil {
		return e
	}
	//format all vals at once
	ret, e := stmt.Exec(vals...)
	if e != nil {
		return e
	}
	row, e := ret.RowsAffected()
	if e != nil {
		return e
	}
	if int(row) != len(scms) {
		return errors.New(fmt.Sprintf("bad insert row %d", row))
	}
	db.Close()
	return nil
}
