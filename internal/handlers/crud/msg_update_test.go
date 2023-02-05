// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMsgUpdate(t *testing.T) {
	ctx, storage, mock, err := setupTestUtil(t)
	require.NoError(t, err)
	t.Run("updateMany", func(t *testing.T) {
		row := mock.NewRows([]string{"count"}).AddRow(1)

		mock.ExpectQuery("SELECT count(*) FROM \"testDatabase\".\"testCollection\" WHERE \"item\" = 'test'").WillReturnRows(row)
		mock.ExpectExec("UPDATE \"testDatabase\".\"testCollection\"  SET \"item\" = 'new test'  WHERE \"item\" = 'test' AND ( NOT (   \"item\" = 'new test') OR (\"item\" IS UNSET )) ").WillReturnResult(sqlmock.NewResult(1, 1))

		updateReq := types.MustMakeDocument(
			"update", "testCollection",
			"updates", types.MustNewArray(
				types.MustMakeDocument(
					"q", types.MustMakeDocument(
						"item", "test",
					),
					"u", types.MustMakeDocument(
						"$set", types.MustMakeDocument(
							"item", "new test",
						),
					),
					"multi", true,
				),
			),
			"ordered", true,
			"$db", "testDatabase",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{updateReq},
		})
		require.NoError(t, err)

		msg, err := storage.MsgUpdate(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"n", int32(1),
			"nModified", int32(1),
			"ok", float64(1),
		)

		actual, _ := msg.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("updateOne", func(t *testing.T) {
		countRow := sqlmock.NewRows([]string{"count"}).AddRow(1)
		idRow := sqlmock.NewRows([]string{"_id"}).AddRow("{\"_id\": 123}")

		mock.ExpectQuery("SELECT count(*) FROM \"testDatabase\".\"testCollection\" WHERE \"item\" = 'test'").WillReturnRows(countRow)
		mock.ExpectQuery("SELECT {\"_id\": \"_id\"} FROM \"testDatabase\".\"testCollection\" WHERE \"item\" = 'test' AND ( NOT (   \"item\" = 'new test') OR (\"item\" IS UNSET )) ").WillReturnRows(idRow)
		mock.ExpectExec("UPDATE \"testDatabase\".\"testCollection\"  SET \"item\" = 'new test' WHERE \"_id\" = 123").WillReturnResult(sqlmock.NewResult(1, 1))

		updateReq := types.MustMakeDocument(
			"update", "testCollection",
			"updates", types.MustNewArray(
				types.MustMakeDocument(
					"q", types.MustMakeDocument(
						"item", "test",
					),
					"u", types.MustMakeDocument(
						"$set", types.MustMakeDocument(
							"item", "new test",
						),
					),
				),
			),
			"ordered", true,
			"$db", "testDatabase",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{updateReq},
		})
		require.NoError(t, err)

		msg, err := storage.MsgUpdate(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"n", int32(1),
			"nModified", int32(1),
			"ok", float64(1),
		)

		actual, _ := msg.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

}
