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

	"github.com/FerretDB/FerretDB/internal/handlers/common"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/wire"
)

// MsgDelete deletes document.
func (h *storage) MsgDelete(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	fmt.Println("msg")
	fmt.Println(msg)
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	fmt.Println("Document")
	fmt.Println(document)
	m := document.Map()
	fmt.Println("m")
	fmt.Println(m)
	collection := m[document.Command()].(string)
	//db := m["$db"].(string)
	docs, _ := m["deletes"].(*types.Array)
	fmt.Println("docs")
	fmt.Println(docs)
	var deleted int32
	for i := 0; i < docs.Len(); i++ {
		doc, err := docs.Get(i)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
		fmt.Println("doc")
		fmt.Println(doc)
		d := doc.(types.Document).Map()
		fmt.Println("d")
		fmt.Println(d)
		sql := fmt.Sprintf(`DELETE FROM %s`, collection)
		//var placeholder pg.Placeholder

		delSQL, args, err := whereHANA(d["q"].(types.Document))
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
		fmt.Println("delSQL")
		fmt.Println(delSQL)
		sql += delSQL
		//limit, _ := d["limit"].(int32)
		//if limit != 0 {
		//	sql += fmt.Sprintf(" WHERE _jsonb->'_id' IN (SELECT _jsonb->'_id' FROM %s", pgx.Identifier{db, collection}.Sanitize())
		//	sql += delSQL
		//	sql += " LIMIT 1)"
		//} else {
		//	sql += delSQL
		//}
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
