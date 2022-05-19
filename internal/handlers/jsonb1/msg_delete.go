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

	"github.com/lucboj/FerretDB_SAP_HANA/internal/handlers/common"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/types"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/util/lazyerrors"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/wire"
)

// MsgDelete deletes document.
func (h *storage) MsgDelete(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {

	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	m := document.Map()

	collection := m[document.Command()].(string)

	docs, _ := m["deletes"].(*types.Array)
	fmt.Println(docs)
	fmt.Println(docs.Len())
	var deleted int32
	for i := 0; i < docs.Len(); i++ {
		doc, err := docs.Get(i)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		d := doc.(types.Document).Map()

		sql := fmt.Sprintf(`DELETE FROM %s`, collection)

		limit, _ := d["limit"].(int32)

		var delSQL string
		var args []any
		if limit != 0 {
			qSQL := fmt.Sprintf("SELECT \"_id\".\"oid\" FROM %s", collection)
			fmt.Println(qSQL)
			whereSQL, whereArgs, err := whereHANA(d["q"].(types.Document))
			qSQL += fmt.Sprintf(whereSQL, whereArgs...) + " LIMIT 1"
			fmt.Println(qSQL)
			rows, err := h.hanaPool.QueryContext(ctx, qSQL)
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
			fmt.Println(rows)
			defer rows.Close()
			var objectID string
			for rows.Next() {
				err = rows.Scan(&objectID)
				fmt.Println(objectID)

			}
			fmt.Println(args)
			args = append(args, objectID)
			fmt.Println(args)
			delSQL = " WHERE \"_id\".\"oid\" = '%s'"

			//objectIDstring = "{\"oid\": \"" + objectIDstring + "\"}"
			//objectID := []byte(objectIDstring)
			//var od fjson.ObjectID
			//
			//err = od.UnmarshalJSON(objectID)
			//if err != nil {
			//	fmt.Println("OH NO")
			//	return nil, lazyerrors.Error(err)
			//}
			//fmt.Println(od)
			//qdocu := types.MustMakeDocument("_id", od)
			//fmt.Println(qdocu)
			//dSQL, args, err := whereHANA(qdocu.(types.ObjectID))
			//fmt.Println(dSQL)
			//fSQL := fmt.Sprintf(dSQL, args...)
			//fmt.Println(fSQL)

		} else {

			fmt.Println(d["q"])
			delSQL, args, err = whereHANA(d["q"].(types.Document))
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
		}

		sql += delSQL

		fmt.Println("SQL")
		fmt.Println(sql)
		fmt.Println(args)
		sqlExec := fmt.Sprintf(sql, args...)
		fmt.Println("sqlExec")
		fmt.Println(sqlExec)
		tag, err := h.hanaPool.ExecContext(ctx, sqlExec)
		if err != nil {
			// TODO check error code
			return nil, common.NewErrorMessage(common.ErrNamespaceNotFound, "MsgDelete: ns not found: %w", err)
		}

		rowsaffected, err := tag.RowsAffected()

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
