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
	"strings"

	"github.com/DocStore/HANA_HWY/internal/bson"
	"github.com/DocStore/HANA_HWY/internal/handlers/common"
	"github.com/DocStore/HANA_HWY/internal/types"
	"github.com/DocStore/HANA_HWY/internal/util/lazyerrors"
	"github.com/DocStore/HANA_HWY/internal/wire"
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
	docs, _ := m["updates"].(*types.Array)

	var selected, updated int32
	for i := 0; i < docs.Len(); i++ {
		doc, err := docs.Get(i)

		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		docM := doc.(types.Document).Map()

		whereSQL, args, err := whereHANA(docM["q"].(types.Document))

		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		updateSQL, updateargs, err := updateMany(docM["u"].(types.Document))

		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		if docM["multi"] != true {

			sql := fmt.Sprintf("select \"_id\".\"oid\" FROM %s", collection)
			sql += whereSQL + " AND NOT (" + fmt.Sprintf(updateSQL, updateargs...) + ")" + " limit 1"

			row := h.hanaPool.QueryRowContext(ctx, fmt.Sprintf(sql, args...))

			var objectID string

			err = row.Scan(&objectID)
			if err != nil {
				return nil, lazyerrors.Error(err)
			}

			countSQL := fmt.Sprintf("SELECT count(*) FROM %s", collection) + whereSQL
			countRow := h.hanaPool.QueryRowContext(ctx, fmt.Sprintf(countSQL, args...))

			err = countRow.Scan(&selected)
			if err != nil {
				return nil, lazyerrors.Error(err)
			}

			whereSQL = "WHERE \"_id\".\"oid\" = '%s'"
			var emptySlice []any
			args = append(emptySlice, objectID)
		}

		sql := fmt.Sprintf("UPDATE %s SET ", collection)

		sql += fmt.Sprintf(updateSQL, updateargs...) + " " + fmt.Sprintf(whereSQL, args...)

		tag, err := h.hanaPool.ExecContext(ctx, sql)
		if err != nil {
			return nil, err
		}

		if docM["multi"] != true {
			updated = 1
		} else {
			rowsaffected, _ := tag.RowsAffected()

			updated += int32(rowsaffected)
			selected = updated
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

func updateMany(updateVal types.Document) (updateSQL string, updateargs []any, err error) {

	updateValMap := updateVal.Map()

	if _, ok := updateValMap["$set"]; !ok {
		return "", nil, common.NewErrorMessage(common.ErrCommandNotFound, "no such command: replaceOne")
	}

	updateVal = updateValMap["$set"].(types.Document)

	for key := range updateVal.Map() {

		if strings.Contains(key, ".") {
			split := strings.Split(key, ".")
			count := 0
			for _, s := range split {
				if (len(split) - 1) == count {
					updateSQL += "\"" + s + "\""
				} else {
					updateSQL += "\"" + s + "\"."
				}
				count += 1
			}
		} else {
			updateSQL += "\"" + key + "\""
		}

		updateSQL += " = "

		value, _ := updateVal.Get(key)
		switch value := value.(type) {
		case string:
			updateargs = append(updateargs, value)
			updateSQL += "'%s'"
		case int64:
			updateargs = append(updateargs, value)
		case int32:

			updateSQL += "%d"
			updateargs = append(updateargs, value)
		case types.Document:
			updateSQL += "%s"
			argDoc, err := whereDocument(value)
			if err != nil {
				return "", nil, lazyerrors.Errorf("scalar: %w", err)
			}

			updateargs = append(updateargs, argDoc)
		case types.ObjectID:
			updateSQL += "%s"
			var bOBJ []byte
			if bOBJ, err = bson.ObjectID(value).MarshalJSONHANA(); err != nil {
				err = lazyerrors.Errorf("scalar: %w", err)
			}
			updateargs = append(updateargs, string(bOBJ))
		default:
			return "", nil, lazyerrors.Errorf("scalar: %w did not fit any case")
		}
	}

	return
}
