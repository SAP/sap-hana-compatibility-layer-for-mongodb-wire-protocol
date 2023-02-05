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
	"fmt"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/fjson"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/handlers/common"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
)

// MsgUpdate modifies an existing document or documents in a collection.
func (h *storage) MsgUpdate(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	unimplementedFields := []string{
		"upsert",
		"writeConcern",
		"collation",
		"arrayFilter",
		"hint",
		"commented",
		"bypassDocumentValidation",
	}

	if err := common.Unimplemented(&document, unimplementedFields...); err != nil {
		return nil, err
	}

	common.Ignored(&document, h.l, "ordered")

	m := document.Map()
	collection := m["update"].(string)
	db := m["$db"].(string)
	docs, _ := m["updates"].(*types.Array)

	var selected, updated, matched int32
	for i := 0; i < docs.Len(); i++ {
		doc, err := docs.Get(i)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		docM := doc.(types.Document).Map()

		whereSQL, err := common.CreateWhereClause(docM["q"].(types.Document))
		if err != nil {
			return nil, err
		}
		// notWhereSQL makes sure we do not update documents which do not need an update
		updateSQL, notWhereSQL, err := common.Update(docM["u"].(types.Document))
		if err != nil {
			return nil, err
		}

		// Get amount of documents that fits the filter. MatchCount
		countSQL := fmt.Sprintf("SELECT count(*) FROM \"%s\".\"%s\"", db, collection) + whereSQL
		countRow := h.hanaPool.QueryRowContext(ctx, countSQL)

		err = countRow.Scan(&matched)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		var args []any
		if docM["multi"] != true { // If updateOne()

			// We get the _id of the one document to update.
			sql := fmt.Sprintf("SELECT {\"_id\": \"_id\"} FROM \"%s\".\"%s\"", db, collection)
			sql += whereSQL + notWhereSQL + " LIMIT 1"
			row := h.hanaPool.QueryRowContext(ctx, sql)

			var objectID []byte

			err = row.Scan(&objectID)
			if err != nil {
				selected += matched
				err = nil
				continue
			}

			id, err := fjson.Unmarshal(objectID)
			if err != nil {
				return nil, err
			}

			updateId, err := common.GetUpdateValue(id.(types.Document).Map()["_id"])
			if err != nil {
				return nil, err
			}

			whereSQL = "WHERE \"_id\" = %s"
			var emptySlice []any
			args = append(emptySlice, updateId)
			notWhereSQL = ""
		}

		sql := fmt.Sprintf("UPDATE \"%s\".\"%s\" ", db, collection)

		sql += updateSQL + " " + fmt.Sprintf(whereSQL, args...) + notWhereSQL

		tag, err := h.hanaPool.ExecContext(ctx, sql)
		if err != nil {
			return nil, err
		}

		// Set modifiedCount
		if docM["multi"] != true {
			updated += 1
			selected += matched
		} else {
			rowsaffected, _ := tag.RowsAffected()

			updated += int32(rowsaffected)
			selected += matched
		}
	}

	var reply wire.OpMsg
	err = reply.SetSections(wire.OpMsgSection{
		Documents: []types.Document{types.MustMakeDocument(
			"n", selected,
			"nModified", updated,
			"ok", float64(1),
		)},
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return &reply, nil
}
