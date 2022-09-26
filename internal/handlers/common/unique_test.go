// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/hana"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/testutil"
	"github.com/stretchr/testify/assert"
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

func setupDBMock(t *testing.T) (mock sqlmock.Sqlmock, hPool hana.Hpool, err error) {
	t.Helper()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(QueryMatcherEqualBytes))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	hPool = hana.Hpool{
		db,
	}

	return
}

func TestUnique(t *testing.T) {
	t.Run("Id is unique", func(t *testing.T) {
		mock, hPool, mockErr := setupDBMock(t)

		assert.Nil(t, mockErr)

		ctx := testutil.Ctx(t)

		emptyRow := mock.NewRows([]string{"_id"})

		mock.ExpectQuery("SELECT _id FROM TESTDATABASE.TESTCOLLECTION  WHERE \"_id\" = 123 LIMIT 1").WillReturnRows(emptyRow)

		unique, errMsg, err := IsIdUnique(int64(123), "TESTDATABASE", "TESTCOLLECTION", ctx, &hPool)

		assert.Nil(t, err)
		assert.Nil(t, errMsg)
		assert.True(t, unique)
	})

	t.Run("Id is not unique", func(t *testing.T) {
		mock, hPool, mockErr := setupDBMock(t)

		assert.Nil(t, mockErr)

		ctx := testutil.Ctx(t)

		emptyRow := mock.NewRows([]string{"_id"}).AddRow("62e2bd54510683f9c0bb0d6b")

		mock.ExpectQuery("SELECT _id FROM TESTDATABASE.TESTCOLLECTION  WHERE \"_id\" = {\"oid\":'62e2bd54510683f9c0bb0d6b'} LIMIT 1").WillReturnRows(emptyRow)

		unique, errMsg, err := IsIdUnique(types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107}, "TESTDATABASE", "TESTCOLLECTION", ctx, &hPool)

		assert.Nil(t, err)
		assert.Equal(t, "E11000 duplicate key error collection: TESTDATABASE.TESTCOLLECTION index: _id_ dup key: { _id: \"62e2bd54510683f9c0bb0d6b\" }", errMsg.Error())
		assert.False(t, unique)
	})
}
