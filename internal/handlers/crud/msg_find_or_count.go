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

type LocatCtx struct {
	exclusion  bool
	filter     types.Document
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
		"allowDiskUse",
		"let",
		"hint",
		"batchSize",
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

	common.Ignored(&document, h.l, "singleBatch")

	docMap := document.Map()
	// Checks if command printshardingstatus is used.
	if isPrintShardingStatus(docMap) {
		return nil, common.NewErrorMessage(common.ErrCommandNotFound, "no such command: printShardingStatus")
	}

	var localCtx LocatCtx
	sql, err := createSqlStmt(docMap, &localCtx)
	if err != nil {
		return nil, err
	}

	rows, err := h.hanaPool.QueryContext(ctx, sql)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return createResponse(docMap, rows, &localCtx)
}

func createSqlStmt(docMap map[string]any, ctx *LocatCtx) (sql string, err error) {
	sql, err = createSqlBaseStmt(docMap, ctx)
	if err != nil {
		return
	}

	whereStmt, err := common.Where(ctx.filter)
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

func createSqlBaseStmt(docMap map[string]any, ctx *LocatCtx) (sql string, err error) {
	_, isFindOp := docMap["find"].(string)
	db := docMap["$db"].(string)

	if isFindOp { // enters here if find
		var projectionSQL string

		projectionIn, _ := docMap["projection"].(types.Document)
		projectionSQL, ctx.exclusion, err = common.Projection(projectionIn)
		if err != nil {
			return
		}

		ctx.collection = docMap["find"].(string)
		ctx.filter, _ = docMap["filter"].(types.Document)
		sql = fmt.Sprintf(`SELECT %s FROM %s.%s`, projectionSQL, db, ctx.collection)
	} else { // enters here if count
		ctx.collection = docMap["count"].(string)
		ctx.filter, _ = docMap["query"].(types.Document)
		sql = fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, db, ctx.collection)
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

			order := sortMap[sortKey].(int32)
			if order == 1 {
				sql += " ASC"
			} else if order == -1 {
				sql += " DESC"
			} else {
				err = common.NewErrorMessage(common.ErrSortBadValue, "")
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

func createResponse(docMap map[string]any, rows *sql.Rows, localCtx *LocatCtx) (resp *wire.OpMsg, err error) {
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

		db := docMap["$db"].(string)
		err = resp.SetSections(wire.OpMsgSection{
			Documents: []types.Document{types.MustMakeDocument(
				"cursor", types.MustMakeDocument(
					"firstBatch", &docs,
					"id", int64(0), // TODO
					"ns", db+"."+localCtx.collection,
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
