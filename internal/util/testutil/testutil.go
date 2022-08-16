// SPDX-FileCopyrightText: 2021 FerretDB Inc.
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

package testutil

import (
	"context"
	"testing"
)

func Ctx(tb testing.TB) context.Context {
	tb.Helper()

	// TODO
	return context.Background()
}

// Try to refactor the tests to all use this for creating handlers
// func setupHandler(t *testing.T, queryMatcher sqlmock.QueryMatcher) (context.Context, *handlers.Handler, sqlmock.Sqlmock) {
// 	t.Helper()

// 	var db *sql.DB
// 	var mock sqlmock.Sqlmock
// 	var err error

// 	if queryMatcher != nil {
// 		db, mock, err = sqlmock.New(sqlmock.QueryMatcherOption(queryMatcher))
// 	} else {
// 		db, mock, err = sqlmock.New()
// 	}

// 	if err != nil {
// 		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
// 	}

// 	defer db.Close()

// 	hPool := hana.Hpool{
// 		db,
// 	}

// 	ctx := Ctx(t)

// 	l := zaptest.NewLogger(t)

// 	crud := crud.NewStorage(&hPool, l)
// 	handler := handlers.New(&handlers.NewOpts{
// 		HanaPool:    &hPool,
// 		Logger:      l,
// 		CrudStorage: crud,
// 		Metrics:     handlers.NewMetrics(),
// 		PeerAddr:    "",
// 	})

// 	return ctx, handler, mock
// }

// Try to refactor the tests to all use this for creating hPool
// func setupHPool(t *testing.T, queryMatcher sqlmock.QueryMatcher) (hana.Hpool, sqlmock.Sqlmock) {
// 	var db *sql.DB
// 	var mock sqlmock.Sqlmock
// 	var err error

// 	if queryMatcher != nil {
// 		db, mock, err = sqlmock.New(sqlmock.QueryMatcherOption(queryMatcher))
// 	} else {
// 		db, mock, err = sqlmock.New()
// 	}
// 	if err != nil {
// 		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
// 	}
// 	defer db.Close()

// 	hPool := hana.Hpool{
// 		db,
// 	}

// 	// ctx := Ctx(t)

// 	// l := zaptest.NewLogger(t)

// 	// storage := crud.NewStorage(&hPool, l)

// 	return hPool, mock
// }

// Try to refactor the tests to all use this for creating QueryMatcher
// var QueryMatcherEqualBytes sqlmock.QueryMatcher = sqlmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {

// 	expectedBytes := []byte(expectedSQL)
// 	actualBytes := []byte(actualSQL)

// 	for i, a := range actualBytes {
// 		if i >= len(expectedBytes) {
// 			return nil
// 		}

// 		e := expectedBytes[i]

// 		if e != a {
// 			return fmt.Errorf(`could not match actual sql: "%s" with expected regexp "%s"`, actualSQL, expectedSQL)
// 		}
// 	}

// 	return nil
// })

// // PoolOpts represents options for creating a connection pool.
// type PoolOpts struct {
// 	// If set, the pool will use read-only user.
// 	ReadOnly bool
// }

// // Pool creates a new connection connection pool for testing.
// func Pool(_ context.Context, tb testing.TB, opts *PoolOpts) *pg.Pool {
// 	tb.Helper()

// 	if testing.Short() {
// 		tb.Skip("skipping in -short mode")
// 	}

// 	username := "postgres"
// 	if opts.ReadOnly {
// 		username = "readonly"
// 	}

// 	pool, err := pg.NewPool("postgres://"+username+"@127.0.0.1:5432/ferretdb?pool_min_conns=1", zaptest.NewLogger(tb), false)
// 	require.NoError(tb, err)
// 	tb.Cleanup(pool.Close)

// 	return pool
// }

// // SchemaName returns a stable schema name for that test.
// func SchemaName(tb testing.TB) string {
// 	return strings.ReplaceAll(strings.ToLower(tb.Name()), "/", "_")
// }

// // Schema creates a new FerretDB database / PostgreSQL schema for testing.
// //
// // Name is stable for that test. It is automatically dropped if test pass.
// func Schema(ctx context.Context, tb testing.TB, pool *pg.Pool) string {
// 	tb.Helper()

// 	if testing.Short() {
// 		tb.Skip("skipping in -short mode")
// 	}

// 	schema := strings.ToLower(tb.Name())
// 	tb.Logf("Using schema %q.", schema)

// 	err := pool.DropSchema(ctx, schema)
// 	if err == pg.ErrNotExist {
// 		err = nil
// 	}
// 	require.NoError(tb, err)

// 	err = pool.CreateSchema(ctx, schema)
// 	require.NoError(tb, err)

// 	tb.Cleanup(func() {
// 		if tb.Failed() {
// 			tb.Logf("Keeping schema %q for debugging.", schema)
// 			return
// 		}

// 		err = pool.DropSchema(ctx, schema)
// 		if err == pg.ErrNotExist { // test might delete it
// 			err = nil
// 		}
// 		require.NoError(tb, err)
// 	})

// 	return schema
// }

// // TableName returns a stable table name for that test.
// func TableName(tb testing.TB) string {
// 	return strings.ReplaceAll(strings.ToLower(tb.Name()), "/", "_")
// }

// // CreateCollection creates FerretDB collection / PostgreSQL table for testing.
// //
// // Name is stable for that test.
// func CreateCollection(ctx context.Context, tb testing.TB, pool *pg.Pool, db string) string {
// 	tb.Helper()

// 	if testing.Short() {
// 		tb.Skip("skipping in -short mode")
// 	}

// 	table := TableName(tb)
// 	tb.Logf("Using table %q.", table)

// 	err := pool.DropTable(ctx, db, table)
// 	if err == pg.ErrNotExist {
// 		err = nil
// 	}
// 	require.NoError(tb, err)

// 	err = pool.CreateCollection(ctx, db, table)
// 	require.NoError(tb, err)

// 	return table
// }
