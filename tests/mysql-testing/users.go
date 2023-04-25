package main

import (
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

type MySQLPersister struct {
	Data []byte
}

var _ mysql_db.MySQLDbPersistence = (*MySQLPersister)(nil)

func (m *MySQLPersister) Persist(ctx *sql.Context, data []byte) error {
	m.Data = data
	return nil
}

func enableUserAccounts(ctx *sql.Context, engine *sqle.Engine) error {
	mysqlDb := engine.Analyzer.Catalog.MySQLDb
	mysqlDb.Enabled = true
	persister := &MySQLPersister{}
	mysqlDb.SetPersister(persister)
	mysqlDb.AddRootAccount()
	dataLoadedFromPretendFile := []byte{
		16, 0, 0, 0, 0, 0, 0, 0, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
		28, 0, 0, 0, 0, 0, 22, 0, 40, 0, 36, 0, 32, 0, 28, 0, 24, 0, 20, 0, 8, 0, 0, 0, 0, 0, 4, 0, 22, 0, 0, 0, 36, 0,
		0, 0, 168, 231, 96, 99, 0, 0, 0, 0, 0, 0, 0, 0, 28, 0, 0, 0, 72, 0, 0, 0, 104, 0, 0, 0, 248, 0, 0, 0, 4, 1, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 41, 0, 0, 0, 42, 54, 66, 66, 52, 56, 51, 55, 69, 66, 55, 52, 51, 50, 57, 49, 48, 53,
		69, 69, 52, 53, 54, 56, 68, 68, 65, 55, 68, 67, 54, 55, 69, 68, 50, 67, 65, 50, 65, 68, 57, 0, 0, 0, 21, 0, 0,
		0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0, 10,
		0, 16, 0, 12, 0, 8, 0, 4, 0, 10, 0, 0, 0, 12, 0, 0, 0, 12, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 30, 0,
		0, 0, 23, 0, 0, 0, 7, 0, 0, 0, 6, 0, 0, 0, 3, 0, 0, 0, 24, 0, 0, 0, 16, 0, 0, 0, 8, 0, 0, 0, 22, 0, 0, 0, 21, 0,
		0, 0, 18, 0, 0, 0, 15, 0, 0, 0, 29, 0, 0, 0, 27, 0, 0, 0, 11, 0, 0, 0, 28, 0, 0, 0, 26, 0, 0, 0, 5, 0, 0, 0, 4,
		0, 0, 0, 2, 0, 0, 0, 30, 0, 0, 0, 25, 0, 0, 0, 20, 0, 0, 0, 19, 0, 0, 0, 14, 0, 0, 0, 13, 0, 0, 0, 17, 0, 0, 0,
		9, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 108, 111, 99, 97, 108, 104, 111, 115, 116, 0, 0, 0,
		8, 0, 0, 0, 103, 109, 115, 95, 117, 115, 101, 114, 0, 0, 0, 0,
	}
	if err := mysqlDb.LoadData(ctx, dataLoadedFromPretendFile); err != nil {
		return err
	}

	return nil
}
