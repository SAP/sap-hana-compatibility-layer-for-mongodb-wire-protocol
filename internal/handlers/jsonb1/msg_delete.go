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

package jsonb1

import (
	"context"
	"fmt"

	"github.wdf.sap.corp/DocStore/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/handlers/common"
	"github.wdf.sap.corp/DocStore/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.wdf.sap.corp/DocStore/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
	"github.wdf.sap.corp/DocStore/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
)

// MsgDelete deletes document.
func (h *storage) MsgDelete(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if err := common.Unimplemented(&document, "let", "writeConcern"); err != nil {
		return nil, err
	}
	common.Ignored(&document, h.l, "ordered")

	m := document.Map()

	collection := m[document.Command()].(string)
	db := m["$db"].(string)

	docs, _ := m["deletes"].(*types.Array)

	var deleted int32
	for i := 0; i < docs.Len(); i++ {

		doc, err := docs.Get(i)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
		check := doc.(types.Document)
		if err := common.Unimplemented(&check, "collation", "hint"); err != nil {
			return nil, err
		}

		d := doc.(types.Document).Map()

		sql := fmt.Sprintf(`DELETE FROM %s.%s`, db, collection)

		limit, _ := d["limit"].(int32)

		var delSQL string
		var args []any
		if limit != 0 { // if deleteOne()
			qSQL := fmt.Sprintf("SELECT \"_id\".\"oid\" FROM %s.%s", db, collection)

			whereSQL, err := common.Where(d["q"].(types.Document))
			if err != nil {
				return nil, lazyerrors.Error(err)
			}

			qSQL += whereSQL + " LIMIT 1"

			rows, err := h.hanaPool.QueryContext(ctx, qSQL)
			if err != nil {
				return nil, lazyerrors.Error(err)
			}

			defer rows.Close()
			var objectID string
			for rows.Next() {
				err = rows.Scan(&objectID)
				if err != nil {
					return nil, lazyerrors.Error(err)
				}

			}

			args = append(args, objectID)
			delSQL = " WHERE \"_id\".\"oid\" = '%s'"

		} else { // if deleteMany()
			delSQL, err = common.Where(d["q"].(types.Document))
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
		}

		sql += delSQL

		sqlExec := fmt.Sprintf(sql, args...)

		tag, err := h.hanaPool.ExecContext(ctx, sqlExec)
		if err != nil {
			// TODO check error code
			return nil, common.NewErrorMessage(common.ErrNamespaceNotFound, "MsgDelete: ns not found: %w", err)
		}

		rowsaffected, err := tag.RowsAffected()
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		deleted += int32(rowsaffected)
	}

	var reply wire.OpMsg
	err = reply.SetSections(wire.OpMsgSection{
		Documents: []types.Document{types.MustMakeDocument(
			"n", deleted,
			"ok", float64(1),
		)},
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return &reply, nil
}
