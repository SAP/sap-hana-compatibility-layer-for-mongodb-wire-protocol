// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package hana

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/testutil"
	"github.com/stretchr/testify/assert"
)

func TestHanapool(t *testing.T) {

	t.Run("create schema", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()

		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		mock.ExpectExec("CREATE SCHEMA database").WillReturnResult(sqlmock.NewResult(1, 1))

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

		mock.ExpectExec("CREATE COLLECTION database.collection").WillReturnResult(sqlmock.NewResult(1, 1))

		h := Hpool{
			db,
		}
		ctx := testutil.Ctx(t)
		err = h.CreateCollection(ctx, "database", "collection")

		assert.Nil(t, err)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

}
