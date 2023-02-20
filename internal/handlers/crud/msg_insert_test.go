// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMsgInsert(t *testing.T) {
	ctx, storage, mock, err := setupTestUtil(t)
	require.NoError(t, err)
	t.Run("insert a document", func(t *testing.T) {
		idRow := mock.NewRows([]string{"_id"})
		args := []driver.Value{[]byte{123, 34, 95, 105, 100, 34, 58, 49, 50, 51, 44, 34, 105, 116, 101, 109, 34, 58, 34, 116, 101, 115, 116, 34, 125}}

		mock.ExpectExec("CREATE SCHEMA \"testDatabase\"").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("CREATE COLLECTION \"testDatabase\".\"testCollection\"").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery("SELECT _id FROM \"testDatabase\".\"testCollection\"  WHERE \"_id\" = 123").WillReturnRows(idRow)
		mock.ExpectExec("INSERT INTO \"testDatabase\".\"testCollection\" VALUES ($1)").WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 1))

		insertReq := types.MustMakeDocument(
			"insert", "testCollection",
			"documents", types.MustNewArray(
				types.MustMakeDocument(
					"_id", int32(123),
					"item", "test",
				),
			),
			"ordered", true,
			"$db", "testDatabase",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{insertReq},
		})
		require.NoError(t, err)

		msg, err := storage.MsgInsert(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"n", int32(1),
			"ok", float64(1),
		)

		actual, _ := msg.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("insert a document. Not unique id", func(t *testing.T) {
		idRow := mock.NewRows([]string{"_id"}).AddRow(123)

		mock.ExpectExec("CREATE SCHEMA \"testDatabase\"").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("CREATE COLLECTION \"testDatabase\".\"testCollection\"").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery("SELECT _id FROM \"testDatabase\".\"testCollection\"  WHERE \"_id\" = 123").WillReturnRows(idRow)

		insertReq := types.MustMakeDocument(
			"insert", "testCollection",
			"documents", types.MustNewArray(
				types.MustMakeDocument(
					"_id", int32(123),
					"item", "test",
				),
			),
			"ordered", true,
			"$db", "testDatabase",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{insertReq},
		})
		require.NoError(t, err)

		msg, err := storage.MsgInsert(ctx, &reqMsg)
		assert.Nil(t, msg)
		assert.EqualError(t, err, "E11000 duplicate key error collection: \"testDatabase\".\"testCollection\" index: _id_ dup key: { _id: 123 }")

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})
}
