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

package crud

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/handlers/common"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
)

type locatCtx struct {
	exclusion  bool
	filter     types.Document
	db         string
	collection string
}

// MsgFindOrCount finds documents in a collection or view and returns a cursor to the selected documents
// or count the number of documents that matches the query filter.
func (h *storage) MsgFindOrCount(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	unimplementedFields := []string{
		"skip",
		"returnKey",
		"showRecordId",
		"tailable",
		"oplogReplay",
		"noCursorTimeout",
		"awaitData",
		"allowPartialResults",
		"collation",
		"let",
		"hint",
		"maxTimeMS",
		"readConcern",
		"max",
		"min",
		"comment",
	}

	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	if err := common.Unimplemented(&document, unimplementedFields...); err != nil {
		return nil, err
	}

	common.Ignored(&document, h.l, "singleBatch", "allowDiskUse", "batchSize")

	docMap := document.Map()
	if isPrintShardingStatus(docMap) {
		return nil, common.NewErrorMessage(common.ErrCommandNotFound, "no such command: printShardingStatus")
	}

	var localCtx locatCtx
	localCtx.db = docMap["$db"].(string)
	sql, err := createSqlStmt(docMap, &localCtx)
	if err != nil {
		return nil, err
	}

	// A workaround which allows connecting and using the basics of some GUI's
	// TODO: Implement this for real.
	if collection, ok := docMap["find"].(string); ok {
		if collection == "system.js" {
			resp := &wire.OpMsg{}
			err = resp.SetSections(wire.OpMsgSection{
				Documents: []types.Document{types.MustMakeDocument(
					"cursor", types.MustMakeDocument(
						"firstBatch", types.MustMakeDocument(),
						"id", int64(0), // TODO
						"ns", localCtx.db+"."+collection,
					),
					"ok", float64(1),
				)},
			})
			if err != nil {
				return nil, err
			}
			return resp, nil
		} else if localCtx.collection == "system.version" {
			resp := &wire.OpMsg{}
			err = resp.SetSections(wire.OpMsgSection{
				Documents: []types.Document{types.MustMakeDocument(
					"cursor", types.MustMakeDocument(
						"firstBatch", types.MustMakeDocument(
							"_id", "featureCompatibilityVersion",
							"version", "5.0",
						),
						"id", int64(0), // TODO
						"ns", localCtx.db+"."+collection,
					),
					"ok", float64(1),
				)},
			})
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
	}

	rows, err := h.hanaPool.QueryContext(ctx, sql)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return createResponse(docMap, rows, &localCtx)
}

func createSqlStmt(docMap map[string]any, ctx *locatCtx) (sql string, err error) {
	sql, err = createSqlBaseStmt(docMap, ctx)
	if err != nil {
		return
	}

	whereStmt, err := common.CreateWhereClause(ctx.filter)
	if err != nil {
		return
	}
	sql += whereStmt

	orderBystmt, err := createOrderByStmt(docMap)
	if err != nil {
		return
	}
	sql += orderBystmt

	limitStmt, err := createLimitStmt(docMap)
	if err != nil {
		return
	}
	sql += limitStmt

	return
}

func createSqlBaseStmt(docMap map[string]any, ctx *locatCtx) (sql string, err error) {
	_, isFindOp := docMap["find"].(string)

	if isFindOp { // enters here if find
		var projectionSQL string

		projectionIn, _ := docMap["projection"].(types.Document)
		projectionSQL, ctx.exclusion, err = common.Projection(projectionIn)
		if err != nil {
			return
		}

		ctx.collection = docMap["find"].(string)
		ctx.filter, _ = docMap["filter"].(types.Document)
		sql = fmt.Sprintf("SELECT %s FROM \"%s\".\"%s\"", projectionSQL, ctx.db, ctx.collection)
	} else { // enters here if count
		ctx.collection = docMap["count"].(string)
		ctx.filter, _ = docMap["query"].(types.Document)
		sql = fmt.Sprintf("SELECT COUNT(*) FROM \"%s\".\"%s\"", ctx.db, ctx.collection)
	}
	return
}

func createOrderByStmt(docMap map[string]any) (sql string, err error) {
	sort, _ := docMap["sort"].(types.Document)
	sortMap := sort.Map()
	if len(sortMap) != 0 {
		sql += " ORDER BY "

		for i, sortKey := range sort.Keys() {
			if i != 0 {
				sql += ","
			}

			if strings.Contains(sortKey, ".") {
				split := strings.Split(sortKey, ".")
				sql += " "
				for j, s := range split {
					if (len(split) - 1) == j {
						sql += "\"" + s + "\""
					} else {
						sql += "\"" + s + "\"."
					}
				}
			} else {
				sql += "\"" + sortKey + "\" "
			}

			order, ok := sortMap[sortKey].(int32)
			if !ok {
				if !anyIsInt(sortMap[sortKey]) {
					err = common.NewErrorMessage(common.ErrSortBadValue, "cannot use type %T for sort", sortMap[sortKey])
					return
				}
				order = int32(sortMap[sortKey].(float64))
			}
			if order == 1 {
				sql += " ASC"
			} else if order == -1 {
				sql += " DESC"
			} else {
				err = common.NewErrorMessage(common.ErrSortBadValue, "cannot use value %s for sort", sortMap[sortKey])
			}
		}
	}
	return
}

func createLimitStmt(docMap map[string]any) (sql string, err error) {
	limit, _ := docMap["limit"].(int32)
	switch {
	case limit == 0:
		// undefined or zero - no limit
	case limit > 0:
		sql += fmt.Sprintf(" LIMIT %d ", limit)
	default:
		err = common.NewErrorMessage(common.ErrNotImplemented, "MsgFind: negative limit values are not supported")
	}
	return
}

func createResponse(docMap map[string]any, rows *sql.Rows, localCtx *locatCtx) (resp *wire.OpMsg, err error) {
	resp = &wire.OpMsg{}
	_, isFindOp := docMap["find"].(string)
	defer rows.Close()
	if isFindOp { //nolint:nestif // FIXME: I have no idead to fix this lint
		var docs types.Array
		var aDoc *types.Document

		for {
			aDoc, err = nextRow(rows)
			if err != nil {
				return nil, lazyerrors.Error(err)
			} else if aDoc == nil {
				break
			}

			if err = docs.Append(*aDoc); err != nil {
				return nil, lazyerrors.Error(err)
			}
		}

		if localCtx.exclusion {
			err = common.ProjectDocuments(&docs, docMap["projection"].(types.Document))
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
		}

		err = resp.SetSections(wire.OpMsgSection{
			Documents: []types.Document{types.MustMakeDocument(
				"cursor", types.MustMakeDocument(
					"firstBatch", &docs,
					"id", int64(0), // TODO
					"ns", localCtx.db+"."+localCtx.collection,
				),
				"ok", float64(1),
			)},
		})
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
	} else {
		var count int32
		for rows.Next() {
			err = rows.Scan(&count)
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
		}

		err = resp.SetSections(wire.OpMsgSection{
			Documents: []types.Document{types.MustMakeDocument(
				"n", count,
				"ok", float64(1),
			)},
		})
	}
	return
}

// Checks if command PrintShardingStatus is being used.
func isPrintShardingStatus(docMap map[string]any) bool {
	if docMap["find"] == "shards" && docMap["$db"] == "config" {
		return true
	} else if docMap["find"] == "mongos" && docMap["$db"] == "config" {
		return true
	} else if docMap["find"] == "version" && docMap["$db"] == "config" {
		return true
	}
	return false
}

// Checks if any is int even if real type is float64. 1.0 would be considered int 1.
func anyIsInt(n any) (ok bool) {
	if nFloat, ok := n.(float64); ok {
		if nFloat == float64(int32(nFloat)) {
			return ok
		}
		ok = false
	}
	return ok
}
