// Copyright 2020-2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// This is an example of how to implement a MySQL server.
// After running the example, you may connect to it using the following:
//
// > mysql --host=localhost --port=3306 --user=root mydb --execute="SELECT * FROM mytable;"
// +----------+-------------------+-------------------------------+----------------------------+
// | name     | email             | phone_numbers                 | created_at                 |
// +----------+-------------------+-------------------------------+----------------------------+
// | Jane Deo | janedeo@gmail.com | ["556-565-566","777-777-777"] | 2022-11-01 12:00:00.000001 |
// | Jane Doe | jane@doe.com      | []                            | 2022-11-01 12:00:00.000001 |
// | John Doe | john@doe.com      | ["555-555-555"]               | 2022-11-01 12:00:00.000001 |
// | John Doe | johnalt@doe.com   | []                            | 2022-11-01 12:00:00.000001 |
// +----------+-------------------+-------------------------------+----------------------------+
//
// The included MySQL client is used in this example, however any MySQL-compatible client will work.

var (
	address = "localhost"
	port    = 3306
)

// For go-mysql-server developers: Remember to update the snippet in the README when this file changes.

func main() {
	ctx := sql.NewEmptyContext()
	engine := sqle.NewDefault(
		memory.NewDBProvider(
			createTestSourceDatabase(ctx),
		),
	)
	if err := enableUserAccounts(ctx, engine); err != nil {
		panic(err)
	}

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		panic(err)
	}
	if err = s.Start(); err != nil {
		panic(err)
	}
}

// func createTestDatabase(ctx *sql.Context) *memory.Database {
// 	db := memory.NewDatabase("test")
// 	db.EnablePrimaryKeyIndexes()

// 	src_table := memory.NewTable("srctable", sql.NewPrimaryKeySchema(sql.Schema{
// 		{Name: "id", Type: sql.Int32, Nullable: false, Source: "testtable", PrimaryKey: true},
// 		{Name: "email", Type: sql.Text, Nullable: false, Source: "testtable"},

// 	}))

// }

func createTestSourceDatabase(ctx *sql.Context) *memory.Database {
	srcdb := memory.NewDatabase("testdb")
	srcdb.EnablePrimaryKeyIndexes()

	srctable := memory.NewTable("srctable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int32, Nullable: false, Source: "srctable", PrimaryKey: true},
		{Name: "name", Type: types.Text, Nullable: false, Source: "srctable"},
		{Name: "email", Type: types.Text, Nullable: false, Source: "srctable"},
	}), srcdb.GetForeignKeyCollection())

	srcdb.AddTable("srctable", srctable)

	dsttable := memory.NewTable("dsttable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int32, Nullable: false, Source: "dsttable", PrimaryKey: true},
		{Name: "name", Type: types.Text, Nullable: false, Source: "dsttable"},
		{Name: "email", Type: types.Text, Nullable: false, Source: "dsttable"},
	}), srcdb.GetForeignKeyCollection())

	srcdb.AddTable("dsttable", dsttable)

	// Identical rows (no insert/update)
	_ = srctable.Insert(ctx, sql.NewRow(int32(1), "IHaventBeenTouched", "unchanged@test.com"))
	_ = dsttable.Insert(ctx, sql.NewRow(int32(1), "IHaventBeenTouched", "unchanged@test.com"))

	// Differing rows (insert, delete)
	_ = srctable.Insert(ctx, sql.NewRow(int32(2), "IHaveBeenCreated", "jane@test.com"))
	_ = dsttable.Insert(ctx, sql.NewRow(int32(3), "IWillBeDeleted", "morgan@test.com"))

	// Differing rows (update)
	_ = srctable.Insert(ctx, sql.NewRow(int32(4), "IHaveBeenUpdated", "steffen@test.com"))
	_ = dsttable.Insert(ctx, sql.NewRow(int32(4), "IWillBeUpdated", "steffen@test.com"))

	return srcdb
}
