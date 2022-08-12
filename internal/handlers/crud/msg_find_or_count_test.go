// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/hana"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/testutil"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestMsgFindOrCound(t *testing.T) {

	t.Run("find documents", func(t *testing.T) {

		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(QueryMatcherEqualBytes))

		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		hPool := hana.Hpool{
			db,
		}

		ctx := testutil.Ctx(t)

		l := zaptest.NewLogger(t)

		storage := NewStorage(&hPool, l)

		docRow := sqlmock.NewRows([]string{"document"}).AddRow([]byte{123, 34, 95, 105, 100, 34, 58, 32, 49, 50, 51, 44, 32, 34, 105, 116, 101, 109, 34, 58, 32, 34, 116, 101, 115, 116, 34, 125})
		mock.ExpectQuery("SELECT * FROM testDatabase.testCollection").WillReturnRows(docRow)

		deleteReq := types.MustMakeDocument(
			"find", "testCollection",
			"filter", types.MustMakeDocument(),
			"$db", "testDatabase",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{deleteReq},
		})
		require.NoError(t, err)

		msg, err := storage.MsgFindOrCount(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"cursor", types.MustMakeDocument(
				"firstBatch", types.MustNewArray(
					types.MustMakeDocument(
						"_id", int32(123),
						"item", "test",
					),
				),
				"id", int64(0),
				"ns", "testDatabase.testCollection",
			),
			"ok", float64(1),
		)

		actual, _ := msg.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}

	})

	t.Run("count", func(t *testing.T) {

		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(QueryMatcherEqualBytes))

		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		hPool := hana.Hpool{
			db,
		}

		ctx := testutil.Ctx(t)

		l := zaptest.NewLogger(t)

		storage := NewStorage(&hPool, l)

		countRow := sqlmock.NewRows([]string{"count"}).AddRow(3)
		mock.ExpectQuery("SELECT COUNT(*) FROM testDatabase.testCollection").WillReturnRows(countRow)

		deleteReq := types.MustMakeDocument(
			"count", "testCollection",
			"query", types.MustMakeDocument(),
			"$db", "testDatabase",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{deleteReq},
		})
		require.NoError(t, err)

		msg, err := storage.MsgFindOrCount(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"n", int32(3),
			"ok", float64(1),
		)

		actual, _ := msg.Document()

		assert.Nil(t, err)
		assert.Equal(t, expected, actual)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}

	})

	t.Run("find documents with where, order by, limit, and projection", func(t *testing.T) {

		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(QueryMatcherEqualBytes))

		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		hPool := hana.Hpool{
			db,
		}

		ctx := testutil.Ctx(t)

		l := zaptest.NewLogger(t)

		storage := NewStorage(&hPool, l)

		idRow := sqlmock.NewRows([]string{"document"}).AddRow([]byte{123, 34, 95, 105, 100, 34, 58, 32, 49, 50, 51, 125})
		mock.ExpectQuery("SELECT {\"_id\": \"_id\"} FROM testDatabase.testCollection WHERE \"item\" = 'test' ORDER BY  \"phone\".\"number\" ASC LIMIT 1").WillReturnRows(idRow)

		deleteReq := types.MustMakeDocument(
			"find", "testCollection",
			"filter", types.MustMakeDocument(
				"item", "test",
			),
			"sort", types.MustMakeDocument(
				"phone.number", int32(1),
			),
			"projection", types.MustMakeDocument(
				"_id", true,
			),
			"limit", int32(1),
			"$db", "testDatabase",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{deleteReq},
		})
		require.NoError(t, err)

		msg, err := storage.MsgFindOrCount(ctx, &reqMsg)
		expected := types.MustMakeDocument(
			"cursor", types.MustMakeDocument(
				"firstBatch", types.MustNewArray(
					types.MustMakeDocument(
						"_id", int32(123),
					),
				),
				"id", int64(0),
				"ns", "testDatabase.testCollection",
			),
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
