// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package hana

import (
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/testutil"
	"github.com/stretchr/testify/assert"
)

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

func TestHanapool(t *testing.T) {
	t.Run("Get tables", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(QueryMatcherEqualBytes))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		row := sqlmock.NewRows([]string{"table_name"}).AddRow("testTable")
		args := []driver.Value{"testDatabase"}
		mock.ExpectExec("CREATE SCHEMA \"testDatabase\"").WillReturnError(fmt.Errorf("error"))
		mock.ExpectQuery("SELECT TABLE_NAME FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = $1 AND TABLE_TYPE = 'COLLECTION';").WithArgs(args...).WillReturnRows(row)

		h := Hpool{
			db,
		}

		ctx := testutil.Ctx(t)
		tables, err := h.Tables(ctx, "testDatabase")

		assert.Nil(t, err)
		assert.Equal(t, []string{"testTable"}, tables)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}

		nilRow := sqlmock.NewRows([]string{"table_name"}).AddRow(nil)
		args = []driver.Value{"testDatabase"}

		mock.ExpectExec("CREATE SCHEMA \"testDatabase\"").WillReturnError(fmt.Errorf("error"))
		mock.ExpectQuery("SELECT TABLE_NAME FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = $1 AND TABLE_TYPE = 'COLLECTION';").WithArgs(args...).WillReturnRows(nilRow)

		tables, err = h.Tables(ctx, "testDatabase")

		assert.Nil(t, tables)
		assert.ErrorContainsf(t, err, "sql: Scan error on column index 0, name \"table_name\": converting NULL to string is unsupported", "")

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("Get schemas", func(t *testing.T) {
		t.Parallel()
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(QueryMatcherEqualBytes))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		h := Hpool{
			db,
		}
		ctx := testutil.Ctx(t)

		schemas := sqlmock.NewRows([]string{"schema_name"}).AddRow("TESTSCHEMA1").AddRow("TESTSCHEMA2")
		mock.ExpectQuery("SELECT SCHEMA_NAME FROM SCHEMAS WHERE SCHEMA_NAME NOT LIKE '%SYS%' AND SCHEMA_OWNER NOT LIKE '%SYS%'").WillReturnRows(schemas)

		actualSchemas, err := h.Schemas(ctx)
		expectedSchemas := []string{"TESTSCHEMA1", "TESTSCHEMA2"}

		assert.Nil(t, err)
		assert.Equal(t, expectedSchemas, actualSchemas)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("create schema", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		mock.ExpectExec("CREATE SCHEMA \"database\"").WillReturnResult(sqlmock.NewResult(1, 1))

		h := Hpool{
			db,
		}
		ctx := testutil.Ctx(t)
		err = h.CreateSchema(ctx, "database")

		assert.Nil(t, err)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("create collection", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		mock.ExpectExec("CREATE COLLECTION \"database\".\"collection\"").WillReturnResult(sqlmock.NewResult(1, 1))

		h := Hpool{
			db,
		}
		ctx := testutil.Ctx(t)
		err = h.CreateCollection(ctx, "database", "collection")

		assert.Nil(t, err)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}

		mock.ExpectExec("CREATE COLLECTION \"database\".\"collection\"").WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(ErrAlreadyExist)

		err = h.CreateCollection(ctx, "database", "collection")

		assert.EqualError(t, err, ErrAlreadyExist.Error())
	})

	t.Run("Drop table", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(QueryMatcherEqualBytes))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		mock.ExpectExec("DROP COLLECTION \"testDatabase\".\"testCollection\"").WillReturnResult(sqlmock.NewResult(1, 1))

		h := Hpool{
			db,
		}

		ctx := testutil.Ctx(t)
		err = h.DropTable(ctx, "testDatabase", "testCollection")

		assert.Nil(t, err)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}

		mock.ExpectExec("DROP COLLECTION \"testDatabase\".\"testCollection\"").WillReturnResult(sqlmock.NewResult(0, 0)).WillReturnError(ErrNotExist)

		err = h.DropTable(ctx, "testDatabase", "testCollection")

		assert.EqualError(t, ErrNotExist, err.Error())
	})

	t.Run("Drop schema", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(QueryMatcherEqualBytes))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		mock.ExpectExec("DROP SCHEMA \"testDatabase\"").WillReturnResult(sqlmock.NewResult(1, 1))

		h := Hpool{
			db,
		}

		ctx := testutil.Ctx(t)
		err = h.DropSchema(ctx, "testDatabase")

		assert.Nil(t, err)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	t.Run("Check availability", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(QueryMatcherEqualBytes))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		row := sqlmock.NewRows([]string{"object_count"}).AddRow(10)

		mock.ExpectQuery("SELECT object_count FROM m_feature_usage WHERE component_name = 'DOCSTORE' AND feature_name = 'COLLECTIONS'").WillReturnRows(row)
		h := Hpool{
			db,
		}

		ctx := testutil.Ctx(t)
		isAvailable, err := h.JSONDocumentStoreAvailable(ctx)

		assert.Nil(t, err)
		assert.Equal(t, true, isAvailable)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})
}
