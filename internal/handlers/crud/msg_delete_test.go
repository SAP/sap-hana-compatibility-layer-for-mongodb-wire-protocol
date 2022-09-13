// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/hana"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/handlers/common"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/testutil"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// QueryMatcherEqualBytes looks if all bytes of actual SQL string is in the expected SQL string.
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

// setupTestUtil sets up context, storage and sqlmock for testing crud operations.
func setupTestUtil(t *testing.T) (context.Context, common.Storage, sqlmock.Sqlmock, error) {
	t.Helper()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(QueryMatcherEqualBytes))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	hPool := hana.Hpool{
		db,
	}

	ctx := testutil.Ctx(t)

	l := zaptest.NewLogger(t)

	storage := NewStorage(&hPool, l)

	return ctx, storage, mock, err
}

func TestMsgDelete(t *testing.T) {
	ctx, storage, mock, err := setupTestUtil(t)
	require.NoError(t, err)
	t.Run("deleteMany", func(t *testing.T) {
		mock.ExpectExec("DELETE FROM testDatabase.testCollection WHERE \"item\" = 'test'").WillReturnResult(sqlmock.NewResult(1, 1))

		deleteReq := types.MustMakeDocument(
			"delete", "testCollection",
			"deletes", types.MustNewArray(
				types.MustMakeDocument(
					"q", types.MustMakeDocument(
						"item", "test",
					),
					"limit", float64(0),
				),
			),
			"ordered", true,
			"$db", "testDatabase",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{deleteReq},
		})
		require.NoError(t, err)

		msg, err := storage.MsgDelete(ctx, &reqMsg)
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

	t.Run("deleteOne", func(t *testing.T) {
		idRow := mock.NewRows([]string{"_id"}).AddRow("{\"_id\": 123}")

		mock.ExpectQuery("SELECT {\"_id\": \"_id\"} FROM testDatabase.testCollection WHERE \"item\" = 'test' LIMIT 1").WillReturnRows(idRow)
		mock.ExpectExec("DELETE FROM testDatabase.testCollection WHERE \"_id\" = 123").WillReturnResult(sqlmock.NewResult(1, 1))

		deleteReq := types.MustMakeDocument(
			"delete", "testCollection",
			"deletes", types.MustNewArray(
				types.MustMakeDocument(
					"q", types.MustMakeDocument(
						"item", "test",
					),
					"limit", int32(1),
				),
			),
			"ordered", true,
			"$db", "testDatabase",
		)

		var reqMsg wire.OpMsg
		err = reqMsg.SetSections(wire.OpMsgSection{
			Documents: []types.Document{deleteReq},
		})
		require.NoError(t, err)

		msg, err := storage.MsgDelete(ctx, &reqMsg)
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
}
