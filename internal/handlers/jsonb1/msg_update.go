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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/DocStore/HANA_HWY/internal/bson"
	"github.com/DocStore/HANA_HWY/internal/fjson"
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

		whereSQL, err := common.Where(docM["q"].(types.Document))
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		updateSQL, notWhereSQL, err := update(docM["u"].(types.Document))
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
		var args []any
		if docM["multi"] != true {

			sql := fmt.Sprintf("select \"_id\" FROM %s", collection)
			sql += whereSQL + notWhereSQL + " limit 1"

			fmt.Println("updateOnesql")
			fmt.Println(sql)
			row := h.hanaPool.QueryRowContext(ctx, sql)

			var objectID []byte

			err = row.Scan(&objectID)
			if err != nil {
				err = nil
				break
			}

			id, err := fjson.Unmarshal(objectID)
			if err != nil {
				return nil, err
			}

			try, err := getUpdateValue(id)
			if err != nil {
				return nil, err
			}

			countSQL := fmt.Sprintf("SELECT count(*) FROM %s", collection) + whereSQL

			countRow := h.hanaPool.QueryRowContext(ctx, countSQL)

			err = countRow.Scan(&selected)
			if err != nil {
				return nil, lazyerrors.Error(err)
			}

			whereSQL = "WHERE \"_id\" = %s"
			var emptySlice []any
			args = append(emptySlice, try)
		}

		sql := fmt.Sprintf("UPDATE %s SET ", collection)

		sql += updateSQL + " " + fmt.Sprintf(whereSQL, args...)
		fmt.Println("updatesql")
		fmt.Println(sql)
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

func update(updateVal types.Document) (updateSQL string, notWhereSQL string, err error) {
	uninmplementedFields := []string{
		"$currentDate",
		"$inc",
		"$min",
		"$max",
		"$mul",
		"$rename",
		"$setOnInsert",
		"$unset",
		"$",
		"$[]",
		"$[<identifier>]",
		"$addToSet",
		"$pop",
		"$pull",
		"$push",
		"$pullAll",
		"$each",
		"$position",
		"$slice",
		"$sort",
		"$bit",
		"$addFields",
		"$project",
		"$unset",
		"$replaceRoot",
		"$replaceWith",
	}

	if err = common.Unimplemented(&updateVal, uninmplementedFields...); err != nil {
		return
	}

	updateValMap := updateVal.Map()

	if _, ok := updateValMap["$set"]; !ok {
		err = common.NewErrorMessage(common.ErrCommandNotFound, "no such command: replaceOne")
		return
	}

	updateVal = updateValMap["$set"].(types.Document)

	var isUnsetSQL string
	var updateValue string
	i := 0
	for key := range updateVal.Map() {

		if i != 0 {
			updateSQL += ", "
			isUnsetSQL += " OR "
		}
		updateKey := getUpdateKey(key)

		value, _ := updateVal.Get(key)

		updateValue, err = getUpdateValue(value)
		if err != nil {
			return
		}

		updateSQL += updateKey + " = " + updateValue
		isUnsetSQL += updateKey + " IS UNSET"
		i++
	}

	notWhereSQL, err = common.Where(updateVal)
	notWhereSQL = " AND ( NOT ( " + strings.Replace(notWhereSQL, "WHERE", "", 1) + ") OR (" + isUnsetSQL + " )) "

	return
}

func getUpdateKey(key string) (updateKey string) {
	if strings.Contains(key, ".") {
		split := strings.Split(key, ".")
		count := 0
		for _, s := range split {
			if (len(split) - 1) == count {
				updateKey += "\"" + s + "\""
			} else {
				updateKey += "\"" + s + "\"."
			}
			count += 1
		}
	} else {
		updateKey += "\"" + key + "\""
	}

	return
}

func getUpdateValue(value any) (updateValue string, err error) {

	var updateArgs []any
	switch value := value.(type) {
	case string:
		updateValue += "'%s'"
		updateArgs = append(updateArgs, value)
	case int64:
		updateValue += "%d"
		updateArgs = append(updateArgs, value)
	case int32:
		updateValue += "%d"
		updateArgs = append(updateArgs, value)
	case float64:
		updateValue += "%f"
		updateArgs = append(updateArgs, value)
	case nil:
		updateValue += "NULL"
		return
	case types.Document:
		updateValue += "%s"
		var argDoc string
		argDoc, err = updateDocument(value)
		if err != nil {
			return
		}

		updateArgs = append(updateArgs, argDoc)
	case types.ObjectID:
		updateValue += "%s"
		var bOBJ []byte
		if bOBJ, err = bson.ObjectID(value).MarshalJSON(); err != nil {
			err = lazyerrors.Errorf("scalar: %w", err)
		}
		oid := bytes.Replace(bOBJ, []byte{34}, []byte{39}, -1)
		oid = bytes.Replace(oid, []byte{39}, []byte{34}, 2)
		updateArgs = append(updateArgs, string(oid))
	default:
		err = lazyerrors.Errorf("Value: %T is not supported for update", value)
	}

	updateValue = fmt.Sprintf(updateValue, updateArgs...)

	return
}

func updateDocument(doc types.Document) (docSQL string, err error) {
	docSQL += "{"
	var value any
	var args []any
	for i, key := range doc.Keys() {

		if i != 0 {
			docSQL += ", "
		}

		docSQL += "\"" + key + "\": "

		value, err = doc.Get(key)

		if err != nil {
			return
		}

		switch value := value.(type) {
		case int32, int64:
			docSQL += "%d"
			args = append(args, value)
		case float64:
			docSQL += "%f"
			args = append(args, value)
		case string:

			docSQL += "'%s'"
			args = append(args, value)
		case bool:

			docSQL += "%t"
			args = append(args, value)
		case nil:
			docSQL += " NULL "
		case types.ObjectID:
			docSQL += "%s"
			var bOBJ []byte
			bOBJ, err = bson.ObjectID(value).MarshalJSON()
			oid := bytes.Replace(bOBJ, []byte{34}, []byte{39}, -1)
			oid = bytes.Replace(oid, []byte{39}, []byte{34}, 2)
			args = append(args, string(oid))
		case types.Document:

			docSQL += "%s"

			var docValue string
			docValue, err = updateDocument(value)
			if err != nil {
				return
			}

			args = append(args, docValue)

		default:

			err = lazyerrors.Errorf("whereDocument does not support this datatype, yet.")
			return
		}
	}

	docSQL = fmt.Sprintf(docSQL, args...) + "}"

	return
}
