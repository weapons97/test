package model

import (
	"fmt"
	"strconv"
	"strings"
)

type SchemaID uint32

type Schema struct {
	ID        SchemaID
	Name      string
	Address   string
	Continent string
}

func NewSchema(line string) (*Schema, error) {
	ss := strings.Split(line, `,`)
	if len(ss) != 4 {
		return nil, fmt.Errorf("bad line %s", line)
	}
	id, e := strconv.Atoi(ss[0])
	if e != nil {
		return nil, e
	}

	scm := &Schema{
		ID:        SchemaID(id),
		Name:      ss[1],
		Address:   ss[2],
		Continent: strings.TrimSpace(ss[3]),
	}
	return scm, nil
}
func (scm *Schema) String() string {
	return fmt.Sprintf("%d,%s,%s,%s",
		scm.ID, scm.Name, scm.Address, scm.Continent)
}
