// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMsgFindAndModify(t *testing.T) {
	ctx, storage, mock, err := setupTestUtil(t)
	require.NoError(t, err)

	t.Run("find document, update and return new document", func(t *testing.T) {

		findDoc := mock.NewRows([]string{"document"}).AddRow([]byte("{\"_id\": 123, \"item\": \"test\"}"))
		findNewDoc := mock.NewRows([]string{"document"}).AddRow([]byte("{\"_id\": 123, \"item\": \"test\", \"name\": \"test name\"}"))
		row1 := mock.NewRows([]string{"count"}).AddRow(1)
		row2 := mock.NewRows([]string{"count"}).AddRow(1)

		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_NAME = 'testDB'").WillReturnRows(row1)
		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = 'testDB' AND table_name = 'testCollection' AND TABLE_TYPE = 'COLLECTION'").WillReturnRows(row2)
		mock.ExpectQuery("SELECT * FROM \"testDB\".\"testCollection\" WHERE \"_id\" = 123 LIMIT 1").WillReturnRows(findDoc)
		mock.ExpectExec("UPDATE \"testDB\".\"testCollection\" SET \"name\" = 'test name' WHERE \"_id\" = 123").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery("SELECT * FROM \"testDB\".\"testCollection\" WHERE \"_id\" = 123 LIMIT 1").WillReturnRows(findNewDoc)

		req := types.MustMakeDocument(
			"findAndModify", "testCollection",
			"query", types.MustMakeDocument(
				"_id", int32(123),
			),
			"remove", false,
			"new", true,
			"upsert", false,
			"update", types.MustMakeDocument(
				"$set", types.MustMakeDocument(
					"name", "test name",
				),
			),

			"$db", "testDB",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{req},
		})
		require.NoError(t, err)

		resp, err := storage.MsgFindAndModify(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"lastErrorObject", types.MustMakeDocument(
				"n", int32(1),
				"updatedExisting", true,
			),
			"value", types.MustMakeDocument(
				"_id", int32(123),
				"item", "test",
				"name", "test name",
			),
			"ok", float64(1),
		)

		actual, _ := resp.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("find document, remove and return removed document", func(t *testing.T) {

		findDoc := mock.NewRows([]string{"document"}).AddRow([]byte("{\"_id\": 123, \"item\": \"test\"}"))
		row1 := mock.NewRows([]string{"count"}).AddRow(1)
		row2 := mock.NewRows([]string{"count"}).AddRow(1)

		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_NAME = 'testDB'").WillReturnRows(row1)
		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = 'testDB' AND table_name = 'testCollection' AND TABLE_TYPE = 'COLLECTION'").WillReturnRows(row2)

		mock.ExpectQuery("SELECT * FROM \"testDB\".\"testCollection\" WHERE \"_id\" = 123 LIMIT 1").WillReturnRows(findDoc)
		mock.ExpectExec("DELETE FROM \"testDB\".\"testCollection\" WHERE \"_id\" = 123").WillReturnResult(sqlmock.NewResult(1, 1))

		req := types.MustMakeDocument(
			"findAndModify", "testCollection",
			"query", types.MustMakeDocument(
				"_id", int32(123),
			),
			"remove", true,
			"new", false,
			"upsert", false,
			"$db", "testDB",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{req},
		})
		require.NoError(t, err)

		resp, err := storage.MsgFindAndModify(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"lastErrorObject", types.MustMakeDocument(
				"n", int32(1),
			),
			"value", types.MustMakeDocument(
				"_id", int32(123),
				"item", "test",
			),
			"ok", float64(1),
		)

		actual, _ := resp.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("find document while sorting, replace and return old document", func(t *testing.T) {

		findDoc := mock.NewRows([]string{"document"}).AddRow([]byte("{\"_id\": 123, \"item\": \"test\"}"))
		row1 := mock.NewRows([]string{"count"}).AddRow(1)
		row2 := mock.NewRows([]string{"count"}).AddRow(1)

		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_NAME = 'testDB'").WillReturnRows(row1)
		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = 'testDB' AND table_name = 'testCollection' AND TABLE_TYPE = 'COLLECTION'").WillReturnRows(row2)

		mock.ExpectQuery("SELECT * FROM \"testDB\".\"testCollection\" WHERE \"_id\" = 123 ORDER BY \"item\"  ASC LIMIT 1").WillReturnRows(findDoc)
		mock.ExpectExec("DELETE FROM \"testDB\".\"testCollection\" WHERE \"_id\" = 123").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("INSERT INTO \"testDB\".\"testCollection\" VALUES ($1) ").WillReturnResult(sqlmock.NewResult(1, 1))

		req := types.MustMakeDocument(
			"findAndModify", "testCollection",
			"query", types.MustMakeDocument(
				"_id", int32(123),
			),
			"remove", false,
			"new", false,
			"upsert", false,
			"sort", types.MustMakeDocument(
				"item", int32(1),
			),
			"update", types.MustMakeDocument(
				"name", "test name",
			),

			"$db", "testDB",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{req},
		})
		require.NoError(t, err)

		resp, err := storage.MsgFindAndModify(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"lastErrorObject", types.MustMakeDocument(
				"n", int32(1),
				"updatedExisting", true,
			),
			"value", types.MustMakeDocument(
				"_id", int32(123),
				"item", "test",
			),
			"ok", float64(1),
		)

		actual, _ := resp.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("find, remove and return old document - No document found", func(t *testing.T) {
		row1 := mock.NewRows([]string{"count"}).AddRow(1)
		row2 := mock.NewRows([]string{"count"}).AddRow(1)

		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_NAME = 'testDB'").WillReturnRows(row1)
		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = 'testDB' AND table_name = 'testCollection' AND TABLE_TYPE = 'COLLECTION'").WillReturnRows(row2)

		mock.ExpectQuery("SELECT * FROM \"testDB\".\"testCollection\" WHERE \"_id\" = 123 LIMIT 1").WillReturnError(sql.ErrNoRows)

		req := types.MustMakeDocument(
			"findAndModify", "testCollection",
			"query", types.MustMakeDocument(
				"_id", int32(123),
			),
			"remove", true,
			"new", false,
			"upsert", false,
			"$db", "testDB",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{req},
		})
		require.NoError(t, err)

		resp, err := storage.MsgFindAndModify(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"lastErrorObject", types.MustMakeDocument(
				"n", int32(0),
			),
			"ok", float64(1),
		)

		actual, _ := resp.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("find, update and return new document - No document found", func(t *testing.T) {
		row1 := mock.NewRows([]string{"count"}).AddRow(1)
		row2 := mock.NewRows([]string{"count"}).AddRow(1)

		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_NAME = 'testDB'").WillReturnRows(row1)
		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = 'testDB' AND table_name = 'testCollection' AND TABLE_TYPE = 'COLLECTION'").WillReturnRows(row2)

		mock.ExpectQuery("SELECT * FROM \"testDB\".\"testCollection\" WHERE \"_id\" = 123 LIMIT 1").WillReturnError(sql.ErrNoRows)

		req := types.MustMakeDocument(
			"findAndModify", "testCollection",
			"query", types.MustMakeDocument(
				"_id", int32(123),
			),
			"remove", false,
			"new", true,
			"upsert", false,
			"update", types.MustMakeDocument(
				"$set", types.MustMakeDocument(
					"name", "test name",
				),
			),

			"$db", "testDB",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{req},
		})
		require.NoError(t, err)

		resp, err := storage.MsgFindAndModify(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"lastErrorObject", types.MustMakeDocument(
				"n", int32(0),
				"updatedExisting", false,
			),
			"value", nil,
			"ok", float64(1),
		)

		actual, _ := resp.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("find, update and return new document - No document found - upsert", func(t *testing.T) {
		row1 := mock.NewRows([]string{"count"}).AddRow(1)
		row2 := mock.NewRows([]string{"count"}).AddRow(1)

		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"SCHEMAS\" WHERE SCHEMA_NAME = 'testDB'").WillReturnRows(row1)
		mock.ExpectQuery("SELECT COUNT(*) FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = 'testDB' AND table_name = 'testCollection' AND TABLE_TYPE = 'COLLECTION'").WillReturnRows(row2)

		upsertDoc := mock.NewRows([]string{"document"}).AddRow([]byte("{\"_id\": 123, \"name\": \"test name\"}"))

		mock.ExpectQuery("SELECT * FROM \"testDB\".\"testCollection\" WHERE \"_id\" = 123 LIMIT 1").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("SELECT _id FROM \"testDB\".\"testCollection\"  WHERE \"_id\" = 123 LIMIT 1").WillReturnError(sql.ErrNoRows)
		mock.ExpectExec("INSERT INTO \"testDB\".\"testCollection\" VALUES ($1) ").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery("SELECT * FROM \"testDB\".\"testCollection\" WHERE \"_id\" = 123 LIMIT 1").WillReturnRows(upsertDoc)

		req := types.MustMakeDocument(
			"findAndModify", "testCollection",
			"query", types.MustMakeDocument(
				"_id", int32(123),
			),
			"remove", false,
			"new", true,
			"upsert", true,
			"update", types.MustMakeDocument(
				"$set", types.MustMakeDocument(
					"name", "test name",
				),
			),

			"$db", "testDB",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{req},
		})
		require.NoError(t, err)

		resp, err := storage.MsgFindAndModify(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"lastErrorObject", types.MustMakeDocument(
				"n", int32(1),
				"updatedExisting", true,
			),
			"value", types.MustMakeDocument(
				"_id", int32(123),
				"name", "test name",
			),
			"ok", float64(1),
		)

		actual, _ := resp.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})
}
