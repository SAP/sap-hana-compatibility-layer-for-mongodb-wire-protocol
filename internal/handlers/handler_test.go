// SPDX-FileCopyrightText: 2021 FerretDB Inc.
//
// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

// Copyright 2021 FerretDB Inc.
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

package handlers

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/bson"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/hana"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/handlers/crud"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/testutil"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/version"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
)

func setup(t *testing.T, qMatcher sqlmock.QueryMatcher) (context.Context, *Handler, sqlmock.Sqlmock) {
	t.Helper()

	var db *sql.DB
	var mock sqlmock.Sqlmock

	if qMatcher != nil {
		db, mock, _ = sqlmock.New(sqlmock.QueryMatcherOption(qMatcher))
	} else {
		db, mock, _ = sqlmock.New()
	}

	hPool := hana.Hpool{
		db,
	}

	ctx := testutil.Ctx(t)

	l := zaptest.NewLogger(t)

	crud := crud.NewStorage(&hPool, l)
	handler := New(&NewOpts{
		HanaPool:    &hPool,
		Logger:      l,
		CrudStorage: crud,
		Metrics:     NewMetrics(),
		PeerAddr:    "",
	})

	return ctx, handler, mock
}

func handle(ctx context.Context, t *testing.T, handler *Handler, req types.Document) types.Document {
	t.Helper()

	reqHeader := wire.MsgHeader{
		RequestID: 1,
		OpCode:    wire.OP_MSG,
	}

	var reqMsg wire.OpMsg
	err := reqMsg.SetSections(wire.OpMsgSection{
		Documents: []types.Document{req},
	})
	require.NoError(t, err)

	_, resBody, _ := handler.Handle(ctx, &reqHeader, &reqMsg)

	actual, err := resBody.(*wire.OpMsg).Document()
	require.NoError(t, err)

	return actual
}

var QueryMatcherEqualBytes sqlmock.QueryMatcher = sqlmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {
	expectedBytes := []byte(expectedSQL)
	actualBytes := []byte(actualSQL)

	for i, a := range actualBytes {
		if i >= len(expectedBytes) {
			return nil
		}

		e := expectedBytes[i]

		if e != a {
			return fmt.Errorf(`could not match actual sql: "%s" with expected regexp "%s"`, actualSQL, expectedSQL)
		}
	}

	return nil
})

func TestFind(t *testing.T) {
	// ctx, handler := setup(t)

	t.Run("find document. None found.", func(t *testing.T) {
		t.Parallel()

		ctx, handler, mock := setup(t, QueryMatcherEqualBytes)

		reqDoc := types.MustMakeDocument(
			"find", "actor",
			"$db", "databaseName",
			"filter", types.MustMakeDocument(
				"last_name", "Doe",
				"actor_id", types.MustMakeDocument(
					"$gt", int32(50),
					"$lt", int32(100),
				),
			),
		)

		row1 := sqlmock.NewRows([]string{"object_count"}).AddRow(10)
		row2 := sqlmock.NewRows([]string{"object_count"}).AddRow("actor")
		row3 := sqlmock.NewRows([]string{"document"})
		mock.ExpectQuery("SELECT object_count FROM m_feature_usage WHERE component_name = 'DOCSTORE' AND feature_name = 'COLLECTIONS'").WillReturnRows(row1)
		mock.ExpectQuery("SELECT Table_name FROM PUBLIC.M_TABLES WHERE ").WillReturnRows(row2)
		mock.ExpectQuery("SELECT * FROM databaseName.actor WHERE \"last_name\" = 'Doe' AND \"actor_id\" \u003e 50 AND \"actor_id\" \u003c 100").WillReturnRows(row3)

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"cursor", types.MustMakeDocument(
				"firstBatch", types.MustNewArray(),
				"id", int64(0),
				"ns", "databaseName"+".actor",
			),
			"ok", float64(1),
		)

		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("find document. Collection not existing.", func(t *testing.T) {
		t.Parallel()

		ctx, handler, mock := setup(t, QueryMatcherEqualBytes)

		reqDoc := types.MustMakeDocument(
			"find", "actor",
			"$db", "databaseName",
			"filter", types.MustMakeDocument(
				"last_name", "Doe",
				"actor_id", types.MustMakeDocument(
					"$gt", int32(50),
					"$lt", int32(100),
				),
			),
		)

		row1 := sqlmock.NewRows([]string{"object_count"}).AddRow(10)
		row2 := sqlmock.NewRows([]string{"Table_name"})

		mock.ExpectQuery("SELECT object_count FROM m_feature_usage WHERE component_name = 'DOCSTORE' AND feature_name = 'COLLECTIONS'").WillReturnRows(row1)
		mock.ExpectQuery("SELECT Table_name FROM PUBLIC.M_TABLES WHERE ").WillReturnRows(row2)

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"ok", float64(0),
			"errmsg", "\u003chandler.go:170 handlers.(*Handler).handleOpMsg\u003e \u003chandler.go:240 handlers.(*Handler).msgStorage\u003e Collection ACTOR does not exist",
			"code", int32(1),
			"codeName", "InternalError",
		)

		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})
}

func TestInsert(t *testing.T) {
	t.Run("insert document. Collection and schema not existing", func(t *testing.T) {
		t.Parallel()

		ctx, handler, mock := setup(t, QueryMatcherEqualBytes)

		reqDoc := types.MustMakeDocument(
			"insert", "test",
			"documents", types.MustNewArray(
				types.MustMakeDocument(
					"_id", int32(1),
					"new", "test",
				),
			),
			"ordered", true,
			"$db", "testDatabase",
		)

		row1 := sqlmock.NewRows([]string{"object_count"}).AddRow(10)
		row2 := sqlmock.NewRows([]string{"Table_name"})
		row3 := sqlmock.NewRows([]string{"_id"})
		args := []driver.Value{[]byte{123, 34, 95, 105, 100, 34, 58, 49, 44, 34, 110, 101, 119, 34, 58, 34, 116, 101, 115, 116, 34, 125}}

		mock.ExpectQuery("SELECT object_count FROM m_feature_usage WHERE component_name = 'DOCSTORE' AND feature_name = 'COLLECTIONS'").WillReturnRows(row1)
		mock.ExpectQuery("SELECT Table_name FROM PUBLIC.M_TABLES WHERE ").WillReturnRows(row2)
		mock.ExpectExec("CREATE SCHEMA testDatabase").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("CREATE COLLECTION testDatabase.test").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery("SELECT _id FROM testDatabase.test  WHERE \"_id\" = 1 LIMIT 1").WillReturnRows(row3)
		mock.ExpectExec("INSERT INTO testDatabase.test VALUES ($1)").WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 1))

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"n", int32(1),
			"ok", float64(1),
		)

		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("insert document", func(t *testing.T) {
		t.Parallel()

		ctx, handler, mock := setup(t, QueryMatcherEqualBytes)

		reqDoc := types.MustMakeDocument(
			"insert", "test",
			"documents", types.MustNewArray(
				types.MustMakeDocument(
					"_id", int32(1),
					"new", "test",
				),
			),
			"ordered", true,
			"$db", "testDatabase",
		)

		row1 := sqlmock.NewRows([]string{"object_count"}).AddRow(10)
		row2 := sqlmock.NewRows([]string{"Table_name"}).AddRow("test")
		row3 := sqlmock.NewRows([]string{"_id"})
		args := []driver.Value{[]byte{123, 34, 95, 105, 100, 34, 58, 49, 44, 34, 110, 101, 119, 34, 58, 34, 116, 101, 115, 116, 34, 125}}

		mock.ExpectQuery("SELECT object_count FROM m_feature_usage WHERE component_name = 'DOCSTORE' AND feature_name = 'COLLECTIONS'").WillReturnRows(row1)
		mock.ExpectQuery("SELECT Table_name FROM PUBLIC.M_TABLES WHERE ").WillReturnRows(row2)
		mock.ExpectQuery("SELECT _id FROM testDatabase.test  WHERE \"_id\" = 1 LIMIT 1").WillReturnRows(row3)
		mock.ExpectExec("INSERT INTO testDatabase.test VALUES ($1)").WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 1))

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"n", int32(1),
			"ok", float64(1),
		)

		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})
}

func TestDatabaseCommand(t *testing.T) {
	t.Run("buildinfo", func(t *testing.T) {
		t.Parallel()

		ctx, handler, _ := setup(t, nil)

		reqDoc := types.MustMakeDocument(
			"buildinfo", int32(1),
			"$db", "testDatabase",
		)

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"version", versionValue,
			"gitVersion", version.Get().Commit,
			"versionArray", types.MustNewArray(int32(5), int32(0), int32(42), int32(0)),
			"bits", int32(strconv.IntSize),
			"debug", version.Get().Debug,
			"maxBsonObjectSize", int32(bson.MaxDocumentLen),
			"ok", float64(1),
			"buildEnvironment", version.Get().BuildEnvironment,
		)

		assert.Equal(t, expected, actual)
	})

	t.Run("create collection", func(t *testing.T) {
		t.Parallel()

		ctx, handler, mock := setup(t, QueryMatcherEqualBytes)

		reqDoc := types.MustMakeDocument(
			"create", "newTest",
			"$db", "testDatabase",
		)

		mock.ExpectExec("CREATE SCHEMA testDatabase").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("CREATE COLLECTION testDatabase.newTest").WillReturnResult(sqlmock.NewResult(1, 1))

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"ok", float64(1),
		)

		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("drop collection", func(t *testing.T) {
		t.Parallel()

		ctx, handler, mock := setup(t, QueryMatcherEqualBytes)

		reqDoc := types.MustMakeDocument(
			"drop", "newTest",
			"$db", "testDatabase",
		)

		mock.ExpectExec("DROP COLLECTION testDatabase.newTest").WillReturnResult(sqlmock.NewResult(1, 1))

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"nIndexesWas", int32(1),
			"ns", "testDatabase.newTest",
			"ok", float64(1),
		)

		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("drop database", func(t *testing.T) {
		t.Parallel()

		ctx, handler, mock := setup(t, QueryMatcherEqualBytes)

		reqDoc := types.MustMakeDocument(
			"dropDatabase", int32(1),
			"$db", "testDatabase",
		)

		mock.ExpectExec("DROP SCHEMA testDatabase").WillReturnResult(sqlmock.NewResult(1, 1))

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"dropped", "testDatabase",
			"ok", float64(1),
		)

		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	// Sometimes fails to due Time
	// t.Run("get log", func(t *testing.T) {
	// 	t.Parallel()

	// 	ctx, handler, mock := setup(t, QueryMatcherEqualBytes)

	// 	reqDoc := types.MustMakeDocument(
	// 		"getLog", "startupWarnings",
	// 		"$db", "admin",
	// 	)

	// 	row := sqlmock.NewRows([]string{"VERSION"}).AddRow(1)

	// 	mock.ExpectQuery("Select VERSION from \"SYS\".\"M_DATABASE\";").WillReturnRows(row)
	// 	strTime := string(time.Now().UTC().Format("2006-01-02T15:04:05.999Z07:00"))
	// 	mv := version.Get()

	// 	actual := handle(ctx, t, handler, reqDoc)
	// 	expected := types.MustMakeDocument(
	// 		"totalLinesWritten", int32(1),
	// 		"log", types.MustNewArray(
	// 			"{\"c\":\"STORAGE\",\"ctx\":\"initandlisten\",\"id\":42000,\"msg\":\"Powered by SAP HANA compatibility layer for MongoDB Wire Protocol "+mv.Version+" and SAP HANA 1.\",\"s\":\"I\",\"t\":{\"$date\":\""+strTime+"\"},\"tags\":[\"startupWarnings\"]}",
	// 		),
	// 		"ok", float64(1),
	// 	)

	// 	assert.Equal(t, expected, actual)

	// 	if err := mock.ExpectationsWereMet(); err != nil {
	// 		t.Errorf("there were unfulfilled expectations: %s", err)
	// 	}

	// })

	t.Run("list collections", func(t *testing.T) {
		t.Parallel()

		ctx, handler, mock := setup(t, QueryMatcherEqualBytes)

		reqDoc := types.MustMakeDocument(
			"listCollections", int32(1),
			"filer", types.MustMakeDocument(),
			"cursor", types.MustMakeDocument(),
			"nameOnly", true,
			"authorizedCollctions", false,
			"$db", "testDatabase",
			"$readPreference", types.MustMakeDocument(
				"mude", "primaryPreferred",
			),
		)

		row := sqlmock.NewRows([]string{"table_name"}).AddRow("testTable")
		args := []driver.Value{"TESTDATABASE"}

		mock.ExpectExec("CREATE SCHEMA testDatabase").WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectQuery("SELECT TABLE_NAME FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = $1 AND TABLE_TYPE = 'COLLECTION';").WithArgs(args...).WillReturnRows(row)

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"cursor", types.MustMakeDocument(
				"id", int64(0),
				"ns", "testDatabase.$cmd.listCollections",
				"firstBatch", types.MustNewArray(
					types.MustMakeDocument(
						"name", "testTable",
						"type", "collection",
					),
				),
			),
			"ok", float64(1),
		)

		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("ping", func(t *testing.T) {
		t.Parallel()

		ctx, handler, _ := setup(t, QueryMatcherEqualBytes)

		reqDoc := types.MustMakeDocument(
			"ping", int32(1),
			"$db", "testDatabase",
		)

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"ok", float64(1),
		)

		assert.Equal(t, expected, actual)
	})

	t.Run("whatsMyUri", func(t *testing.T) {
		t.Parallel()

		ctx, handler, _ := setup(t, QueryMatcherEqualBytes)

		reqDoc := types.MustMakeDocument(
			"whatsmyuri", int32(1),
			"$db", "testDatabase",
		)

		actual := handle(ctx, t, handler, reqDoc)
		expected := types.MustMakeDocument(
			"you", "",
			"ok", float64(1),
		)

		assert.Equal(t, expected, actual)
	})
}

// func TestFind(t *testing.T) {
// 	t.Parallel()
// 	ctx, handler, _ := setup(t, &testutil.PoolOpts{
// 		ReadOnly: true,
// 	})

// 	lastUpdate := time.Date(2020, 2, 15, 9, 34, 33, 0, time.UTC).Local()

// 	type testCase struct {
// 		req  types.Document
// 		resp *types.Array
// 	}

// 	testCases := map[string]testCase{
// 		"ValueLtGt": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"last_name", "HOFFMAN",
// 					"actor_id", types.MustMakeDocument(
// 						"$gt", int32(50),
// 						"$lt", int32(100),
// 					),
// 				),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x4f, 0x00, 0x00, 0x00, 0x4f},
// 					"actor_id", int32(79),
// 					"first_name", "MAE",
// 					"last_name", "HOFFMAN",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 		"InLteGte": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"last_name", types.MustMakeDocument(
// 						"$in", types.MustNewArray("HOFFMAN"),
// 					),
// 					"actor_id", types.MustMakeDocument(
// 						"$gte", int32(50),
// 						"$lte", int32(100),
// 					),
// 				),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x4f, 0x00, 0x00, 0x00, 0x4f},
// 					"actor_id", int32(79),
// 					"first_name", "MAE",
// 					"last_name", "HOFFMAN",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 		"NinEqNe": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"last_name", types.MustMakeDocument(
// 						"$nin", types.MustNewArray("NEESON"),
// 						"$ne", "AKROYD",
// 					),
// 					"first_name", types.MustMakeDocument(
// 						"$eq", "CHRISTIAN",
// 					),
// 				),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x0a},
// 					"actor_id", int32(10),
// 					"first_name", "CHRISTIAN",
// 					"last_name", "GABLE",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 		"Not": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"last_name", types.MustMakeDocument(
// 						"$not", types.MustMakeDocument(
// 							"$eq", "GUINESS",
// 						),
// 					),
// 				),
// 				"sort", types.MustMakeDocument(
// 					"actor_id", int32(1),
// 				),
// 				"limit", int32(1),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02},
// 					"actor_id", int32(2),
// 					"first_name", "NICK",
// 					"last_name", "WAHLBERG",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 		"NestedNot": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"last_name", types.MustMakeDocument(
// 						"$not", types.MustMakeDocument(
// 							"$not", types.MustMakeDocument(
// 								"$not", types.MustMakeDocument(
// 									"$eq", "GUINESS",
// 								),
// 							),
// 						),
// 					),
// 				),
// 				"sort", types.MustMakeDocument(
// 					"actor_id", int32(1),
// 				),
// 				"limit", int32(1),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02},
// 					"actor_id", int32(2),
// 					"first_name", "NICK",
// 					"last_name", "WAHLBERG",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 		"AndOr": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"$and", types.MustNewArray(
// 						types.MustMakeDocument(
// 							"first_name", "CHRISTIAN",
// 						),
// 						types.MustMakeDocument(
// 							"$or", types.MustNewArray(
// 								types.MustMakeDocument(
// 									"last_name", "GABLE",
// 								),
// 								types.MustMakeDocument(
// 									"last_name", "NEESON",
// 								),
// 							),
// 						),
// 					),
// 				),
// 				"sort", types.MustMakeDocument(
// 					"actor_id", int32(1),
// 				),
// 				"limit", int32(1),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x0a},
// 					"actor_id", int32(10),
// 					"first_name", "CHRISTIAN",
// 					"last_name", "GABLE",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 		"Nor": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"$nor", types.MustNewArray(
// 						types.MustMakeDocument("actor_id", types.MustMakeDocument("$gt", int32(2))),
// 						types.MustMakeDocument("first_name", "PENELOPE"),
// 					),
// 				),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02},
// 					"actor_id", int32(2),
// 					"first_name", "NICK",
// 					"last_name", "WAHLBERG",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 		"ValueRegex": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"last_name", types.Regex{Pattern: "hoffman", Options: "i"},
// 				),
// 				"sort", types.MustMakeDocument(
// 					"actor_id", int32(1),
// 				),
// 				"limit", int32(1),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x1c},
// 					"actor_id", int32(28),
// 					"first_name", "WOODY",
// 					"last_name", "HOFFMAN",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 		"Regex": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"last_name", types.MustMakeDocument(
// 						"$regex", types.Regex{Pattern: "hoffman", Options: "i"},
// 					),
// 				),
// 				"sort", types.MustMakeDocument(
// 					"actor_id", int32(1),
// 				),
// 				"limit", int32(1),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x1c},
// 					"actor_id", int32(28),
// 					"first_name", "WOODY",
// 					"last_name", "HOFFMAN",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 		"RegexOptions": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"last_name", types.MustMakeDocument(
// 						"$regex", types.Regex{Pattern: "hoffman"},
// 						"$options", "i",
// 					),
// 				),
// 				"sort", types.MustMakeDocument(
// 					"actor_id", int32(1),
// 				),
// 				"limit", int32(1),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x1c},
// 					"actor_id", int32(28),
// 					"first_name", "WOODY",
// 					"last_name", "HOFFMAN",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 		"RegexStringOptions": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"filter", types.MustMakeDocument(
// 					"last_name", types.MustMakeDocument(
// 						"$regex", "hoffman",
// 						"$options", "i",
// 					),
// 				),
// 				"sort", types.MustMakeDocument(
// 					"actor_id", int32(1),
// 				),
// 				"limit", int32(1),
// 			),
// 			resp: types.MustNewArray(
// 				types.MustMakeDocument(
// 					"_id", types.ObjectID{0x61, 0x2e, 0xc2, 0x80, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x1c},
// 					"actor_id", int32(28),
// 					"first_name", "WOODY",
// 					"last_name", "HOFFMAN",
// 					"last_update", lastUpdate,
// 				),
// 			),
// 		},
// 	}

// 	for name, tc := range testCases { //nolint:paralleltest // false positive
// 		name, tc := name, tc
// 		t.Run(name, func(t *testing.T) {
// 			t.Parallel()

// 			for _, schema := range []string{"monila", "pagila"} {
// 				t.Run(schema, func(t *testing.T) {
// 					// not parallel because we modify tc

// 					tc.req.Set("$db", schema)

// 					if schema == "pagila" {
// 						for i := 0; i < tc.resp.Len(); i++ {
// 							doc, err := tc.resp.Get(i)
// 							require.NoError(t, err)
// 							d := doc.(types.Document)
// 							d.Remove("_id")
// 							err = tc.resp.Set(i, d)
// 							require.NoError(t, err)
// 						}
// 					}

// 					actual := handle(ctx, t, handler, tc.req)
// 					expected := types.MustMakeDocument(
// 						"cursor", types.MustMakeDocument(
// 							"firstBatch", tc.resp,
// 							"id", int64(0),
// 							"ns", schema+".actor",
// 						),
// 						"ok", float64(1),
// 					)
// 					assert.Equal(t, expected, actual)
// 				})
// 			}
// 		})
// 	}
// }

// func TestReadOnlyHandlers(t *testing.T) {
// 	t.Parallel()
// 	ctx, handler := setup(t)

// 	type testCase struct {
// 		req         types.Document
// 		reqSetDB    bool
// 		resp        types.Document
// 		compareFunc func(t testing.TB, req, expected, actual types.Document)
// 	}

// 	hostname, err := os.Hostname()
// 	require.NoError(t, err)

// 	testCases := map[string]testCase{
// 		"BuildInfo": {
// 			req: types.MustMakeDocument(
// 				"buildInfo", int32(1),
// 			),
// 			resp: types.MustMakeDocument(
// 				"version", "5.0.42",
// 				"gitVersion", version.Get().Commit,
// 				"versionArray", types.MustNewArray(int32(5), int32(0), int32(42), int32(0)),
// 				"bits", int32(strconv.IntSize),
// 				"debug", version.Get().Debug,
// 				"maxBsonObjectSize", int32(bson.MaxDocumentLen),
// 				"ok", float64(1),
// 				"buildEnvironment", types.MustMakeDocument(),
// 			),
// 		},

// 		"CollStats": {
// 			req: types.MustMakeDocument(
// 				"collStats", "film",
// 			),
// 			reqSetDB: true,
// 			resp: types.MustMakeDocument(
// 				"ns", "monila.film",
// 				"count", int32(1_000),
// 				"size", int32(1_236_992),
// 				"storageSize", int32(1_204_224),
// 				"totalIndexSize", int32(0),
// 				"totalSize", int32(1_236_992),
// 				"scaleFactor", int32(1),
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, req, expected, actual types.Document) {
// 				db, err := req.Get("$db")
// 				require.NoError(t, err)
// 				if db.(string) == "monila" {
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 30_000, "size")
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 30_000, "storageSize")
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 30_000, "totalSize")
// 					assert.Equal(t, expected, actual)
// 				}
// 			},
// 		},

// 		"CountAllActors": {
// 			req: types.MustMakeDocument(
// 				"count", "actor",
// 			),
// 			reqSetDB: true,
// 			resp: types.MustMakeDocument(
// 				"n", int32(200),
// 				"ok", float64(1),
// 			),
// 		},
// 		"CountExactlyOneActor": {
// 			req: types.MustMakeDocument(
// 				"count", "actor",
// 				"query", types.MustMakeDocument(
// 					"actor_id", int32(28),
// 				),
// 			),
// 			reqSetDB: true,
// 			resp: types.MustMakeDocument(
// 				"n", int32(1),
// 				"ok", float64(1),
// 			),
// 		},
// 		"CountLastNameHoffman": {
// 			req: types.MustMakeDocument(
// 				"count", "actor",
// 				"query", types.MustMakeDocument(
// 					"last_name", "HOFFMAN",
// 				),
// 			),
// 			reqSetDB: true,
// 			resp: types.MustMakeDocument(
// 				"n", int32(3),
// 				"ok", float64(1),
// 			),
// 		},
// 		"DataSize": {
// 			req: types.MustMakeDocument(
// 				"dataSize", "monila.actor",
// 			),
// 			reqSetDB: true,
// 			resp: types.MustMakeDocument(
// 				"estimate", false,
// 				"size", int32(114_688),
// 				"numObjects", int32(200),
// 				"millis", int32(20),
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, req, expected, actual types.Document) {
// 				db, err := req.Get("$db")
// 				require.NoError(t, err)
// 				if db.(string) == "monila" {
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 30, "millis")
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 20_000, "size")
// 					assert.Equal(t, expected, actual)
// 				}
// 			},
// 		},
// 		"DataSizeCollectionNotExist": {
// 			req: types.MustMakeDocument(
// 				"dataSize", "some-database.some-collection",
// 			),
// 			reqSetDB: true,
// 			resp: types.MustMakeDocument(
// 				"size", int32(0),
// 				"numObjects", int32(0),
// 				"millis", int32(20),
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, req, expected, actual types.Document) {
// 				db, err := req.Get("$db")
// 				require.NoError(t, err)
// 				if db.(string) == "monila" {
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 30, "millis")
// 					assert.Equal(t, expected, actual)
// 				}
// 			},
// 		},

// 		"DBStats": {
// 			req: types.MustMakeDocument(
// 				"dbstats", int32(1),
// 			),
// 			reqSetDB: true,
// 			resp: types.MustMakeDocument(
// 				"db", "monila",
// 				"collections", int32(14),
// 				"views", int32(0),
// 				"objects", int32(30224),
// 				"avgObjSize", 437.7342509264161,
// 				"dataSize", 1.323008e+07,
// 				"indexes", int32(0),
// 				"indexSize", float64(0),
// 				"totalSize", 1.3615104e+07,
// 				"scaleFactor", float64(1),
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, req, expected, actual types.Document) {
// 				db, err := req.Get("$db")
// 				require.NoError(t, err)
// 				if db.(string) == "monila" {
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 20, "avgObjSize")
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 400_000, "dataSize")
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 400_000, "totalSize")
// 					assert.Equal(t, expected, actual)
// 				}
// 			},
// 		},
// 		"DBStatsWithScale": {
// 			req: types.MustMakeDocument(
// 				"dbstats", int32(1),
// 				"scale", float64(1_000),
// 			),
// 			reqSetDB: true,
// 			resp: types.MustMakeDocument(
// 				"db", "monila",
// 				"collections", int32(14),
// 				"views", int32(0),
// 				"objects", int32(30224),
// 				"avgObjSize", 437.7342509264161,
// 				"dataSize", 13_230.08,
// 				"indexes", int32(0),
// 				"indexSize", float64(0),
// 				"totalSize", 13_615.104,
// 				"scaleFactor", float64(1_000),
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, req, expected, actual types.Document) {
// 				db, err := req.Get("$db")
// 				require.NoError(t, err)
// 				if db.(string) == "monila" {
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 20, "avgObjSize")
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 400, "dataSize")
// 					testutil.CompareAndSetByPathNum(t, expected, actual, 400, "totalSize")
// 					assert.Equal(t, expected, actual)
// 				}
// 			},
// 		},

// 		"FindProjectionActorsFirstAndLastName": {
// 			req: types.MustMakeDocument(
// 				"find", "actor",
// 				"projection", types.MustMakeDocument(
// 					"first_name", int32(1),
// 					"last_name", int32(1),
// 				),
// 				"filter", types.MustMakeDocument(
// 					"actor_id", int32(28),
// 				),
// 			),
// 			reqSetDB: true,
// 			resp: types.MustMakeDocument(
// 				"cursor", types.MustMakeDocument(
// 					"firstBatch", types.MustNewArray(
// 						types.MustMakeDocument(
// 							"first_name", "WOODY",
// 							"last_name", "HOFFMAN",
// 						),
// 					),
// 					"id", int64(0),
// 					"ns", "", // set by compareFunc
// 				),
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, _, expected, actual types.Document) {
// 				actualV := testutil.GetByPath(t, actual, "cursor", "ns")
// 				testutil.SetByPath(t, expected, actualV, "cursor", "ns")
// 				assert.Equal(t, expected, actual)
// 			},
// 		},

// 		"GetLog": {
// 			req: types.MustMakeDocument(
// 				"getLog", "startupWarnings",
// 			),
// 			resp: types.MustMakeDocument(
// 				"totalLinesWritten", int32(2),
// 				will be replaced with the real value during the test
// 				"log", types.MakeArray(2),
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, _ types.Document, actual, expected types.Document) {
// 				Just testing "ok" response, not the body of the response
// 				actualV := testutil.GetByPath(t, actual, "log")
// 				testutil.SetByPath(t, expected, actualV, "log")
// 				assert.Equal(t, expected, actual)
// 			},
// 		},

// 		"GetParameter": {
// 			req: types.MustMakeDocument(
// 				"getParameter", int32(1),
// 			),
// 			resp: types.MustMakeDocument(
// 				"version", "5.0.42",
// 				"ok", float64(1),
// 			),
// 		},

// 		"ListCommands": {
// 			req: types.MustMakeDocument(
// 				"listCommands", int32(1),
// 			),
// 			resp: types.MustMakeDocument(
// 				"commands", types.MustMakeDocument(),
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, _ types.Document, actual, expected types.Document) {
// 				actualV := testutil.GetByPath(t, actual, "commands")
// 				testutil.SetByPath(t, expected, actualV, "commands")
// 				assert.Equal(t, expected, actual)
// 			},
// 		},

// 		"IsMaster": {
// 			req: types.MustMakeDocument(
// 				"isMaster", int32(1),
// 			),
// 			resp: types.MustMakeDocument(
// 				"helloOk", true,
// 				"ismaster", true,
// 				"maxBsonObjectSize", int32(bson.MaxDocumentLen),
// 				"maxMessageSizeBytes", int32(wire.MaxMsgLen),
// 				"maxWriteBatchSize", int32(100000),
// 				"localTime", time.Now(),
// 				"minWireVersion", int32(13),
// 				"maxWireVersion", int32(13),
// 				"readOnly", false,
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, _ types.Document, actual, expected types.Document) {
// 				testutil.CompareAndSetByPathTime(t, expected, actual, time.Second, "localTime")
// 				assert.Equal(t, expected, actual)
// 			},
// 		},
// 		"Hello": {
// 			req: types.MustMakeDocument(
// 				"hello", int32(1),
// 			),
// 			resp: types.MustMakeDocument(
// 				"helloOk", true,
// 				"ismaster", true,
// 				"maxBsonObjectSize", int32(bson.MaxDocumentLen),
// 				"maxMessageSizeBytes", int32(wire.MaxMsgLen),
// 				"maxWriteBatchSize", int32(100000),
// 				"localTime", time.Now(),
// 				"minWireVersion", int32(13),
// 				"maxWireVersion", int32(13),
// 				"readOnly", false,
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, _ types.Document, actual, expected types.Document) {
// 				testutil.CompareAndSetByPathTime(t, expected, actual, time.Second, "localTime")
// 				assert.Equal(t, expected, actual)
// 			},
// 		},

// 		"HostInfo": {
// 			req: types.MustMakeDocument(
// 				"hostInfo", int32(1),
// 			),
// 			resp: types.MustMakeDocument(
// 				"system", types.MustMakeDocument(
// 					"currentTime", time.Now(),
// 					"hostname", hostname,
// 					"cpuAddrSize", int32(strconv.IntSize),
// 					"numCores", int32(runtime.NumCPU()),
// 					"cpuArch", runtime.GOARCH,
// 					"numaEnabled", false,
// 				),
// 				"os", types.MustMakeDocument(
// 					"type", strings.Title(runtime.GOOS),
// 				),
// 				"ok", float64(1),
// 			),
// 			compareFunc: func(t testing.TB, _ types.Document, actual, expected types.Document) {
// 				testutil.CompareAndSetByPathTime(t, expected, actual, time.Second, "system", "currentTime")
// 				assert.Equal(t, expected, actual)
// 			},
// 		},

// 		"ServerStatus": {
// 			req: types.MustMakeDocument(
// 				"serverStatus", int32(1),
// 			),
// 			resp: types.MustMakeDocument(
// 				"version", "5.0.42",
// 				"ok", float64(1),
// 			),
// 		},
// 	}

// 	for name, tc := range testCases { //nolint:paralleltest // false positive
// 		name, tc := name, tc
// 		t.Run(name, func(t *testing.T) {
// 			t.Parallel()

// 			for _, schema := range []string{"monila", "pagila"} {
// 				t.Run(schema, func(t *testing.T) {
// 					not parallel because we modify tc

// 					if tc.reqSetDB {
// 						tc.req.Set("$db", schema)
// 					}

// 					actual := handle(ctx, t, handler, tc.req)
// 					if tc.compareFunc == nil {
// 						assert.Equal(t, tc.resp, actual)
// 					} else {
// 						tc.compareFunc(t, tc.req, tc.resp, actual)
// 					}
// 				})
// 			}
// 		})
// 	}
// }

//nolint:paralleltest // we test a global list of databases
// func TestListDropDatabase(t *testing.T) {
// 	ctx, handler, pool := setup(t, nil)

// 	t.Run("existing", func(t *testing.T) {
// 		db := testutil.Schema(ctx, t, pool)

// 		actualList := handle(ctx, t, handler, types.MustMakeDocument(
// 			"listDatabases", int32(1),
// 		))
// 		expectedList := types.MustMakeDocument(
// 			"databases", types.MustNewArray(
// 				types.MustMakeDocument(
// 					"name", "monila",
// 					"sizeOnDisk", int64(13_631_488),
// 					"empty", false,
// 				),
// 				types.MustMakeDocument(
// 					"name", "pagila",
// 					"sizeOnDisk", int64(7_127_040),
// 					"empty", false,
// 				),
// 				types.MustMakeDocument(
// 					"name", "test",
// 					"sizeOnDisk", int64(0),
// 					"empty", true,
// 				),
// 				types.MustMakeDocument(
// 					"name", db,
// 					"sizeOnDisk", int64(0),
// 					"empty", true,
// 				),
// 			),
// 			"totalSize", int64(30_286_627),
// 			"totalSizeMb", int64(28),
// 			"ok", float64(1),
// 		)

// 		testutil.CompareAndSetByPathNum(t, expectedList, actualList, 2_000_000, "totalSize")
// 		testutil.CompareAndSetByPathNum(t, expectedList, actualList, 2, "totalSizeMb")

// 		expectedDBs := testutil.GetByPath(t, expectedList, "databases").(*types.Array)
// 		actualDBs := testutil.GetByPath(t, actualList, "databases").(*types.Array)
// 		require.Equal(t, expectedDBs.Len(), actualDBs.Len())
// 		for i := 0; i < actualDBs.Len(); i++ {
// 			actualDB, err := actualDBs.Get(i)
// 			require.NoError(t, err)
// 			expectedDB, err := expectedDBs.Get(i)
// 			require.NoError(t, err)
// 			testutil.CompareAndSetByPathNum(t, expectedDB.(types.Document), actualDB.(types.Document), 500_000, "sizeOnDisk")
// 		}

// 		assert.Equal(t, expectedList, actualList)

// 		actualDrop := handle(ctx, t, handler, types.MustMakeDocument(
// 			"dropDatabase", int32(1),
// 			"$db", db,
// 		))
// 		expectedDrop := types.MustMakeDocument(
// 			"dropped", db,
// 			"ok", float64(1),
// 		)
// 		assert.Equal(t, expectedDrop, actualDrop)

// 		databases := testutil.GetByPath(t, expectedList, "databases").(*types.Array)
// 		databases, err := databases.Subslice(0, databases.Len()-1)
// 		require.NoError(t, err)
// 		testutil.SetByPath(t, expectedList, databases, "databases")

// 		actualList = handle(ctx, t, handler, types.MustMakeDocument(
// 			"listDatabases", int32(1),
// 		))
// 		assert.Equal(t, expectedList, actualList)
// 	})

// 	t.Run("nonexisting", func(t *testing.T) {
// 		actual := handle(ctx, t, handler, types.MustMakeDocument(
// 			"dropDatabase", int32(1),
// 			"$db", "nonexisting",
// 		))
// 		expected := types.MustMakeDocument(
// 			// no $db
// 			"ok", float64(1),
// 		)
// 		assert.Equal(t, expected, actual)
// 	})
// }

// //nolint:paralleltest // we test a global list of collections
// func TestCreateListDropCollection(t *testing.T) {
// 	ctx, handler, pool := setup(t, nil)
// 	db := testutil.Schema(ctx, t, pool)

// 	t.Run("nonexisting", func(t *testing.T) {
// 		collection := testutil.TableName(t)

// 		actual := handle(ctx, t, handler, types.MustMakeDocument(
// 			"create", collection,
// 			"$db", db,
// 		))
// 		expected := types.MustMakeDocument(
// 			"ok", float64(1),
// 		)
// 		assert.Equal(t, expected, actual)

// 		// TODO test listCollections command once we have better cursor support
// 		// https://github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/issues/79

// 		tables, err := pool.Tables(ctx, db)
// 		require.NoError(t, err)
// 		assert.Equal(t, []string{collection}, tables)

// 		actual = handle(ctx, t, handler, types.MustMakeDocument(
// 			"drop", collection,
// 			"$db", db,
// 		))
// 		expected = types.MustMakeDocument(
// 			"nIndexesWas", int32(1),
// 			"ns", db+"."+collection,
// 			"ok", float64(1),
// 		)
// 		assert.Equal(t, expected, actual)

// 		actual = handle(ctx, t, handler, types.MustMakeDocument(
// 			"drop", collection,
// 			"$db", db,
// 		))
// 		expected = types.MustMakeDocument(
// 			"ok", float64(0),
// 			"errmsg", "ns not found",
// 			"code", int32(26),
// 			"codeName", "NamespaceNotFound",
// 		)
// 		assert.Equal(t, expected, actual)
// 	})

// 	t.Run("existing", func(t *testing.T) {
// 		collection := testutil.CreateCollection(ctx, t, pool, db)

// 		actual := handle(ctx, t, handler, types.MustMakeDocument(
// 			"create", collection,
// 			"$db", db,
// 		))
// 		expected := types.MustMakeDocument(
// 			"ok", float64(0),
// 			"errmsg", "Collection already exists. NS: testcreatelistdropcollection.testcreatelistdropcollection_existing",
// 			"code", int32(48),
// 			"codeName", "NamespaceExists",
// 		)
// 		assert.Equal(t, expected, actual)
// 	})
// }
