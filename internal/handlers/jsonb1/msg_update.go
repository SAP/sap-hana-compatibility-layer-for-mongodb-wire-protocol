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

	"github.com/lucboj/FerretDB_SAP_HANA/internal/bson"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/types"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/util/lazyerrors"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/wire"
)

// MsgUpdate modifies an existing document or documents in a collection.
func (h *storage) MsgUpdate(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	m := document.Map()
	collection := m["update"].(string)
	docs, _ := m["updates"].(*types.Array)
	db := m["$db"].(string)

	fmt.Println(document)   // {map,,.
	fmt.Println(m)          // map,,.
	fmt.Println(collection) // test
	fmt.Println(docs)       // &{[{map[q:
	fmt.Println(db)         // BOJER
	fmt.Println(docs.Len()) // often 1

	var selected, updated int32
	for i := 0; i < docs.Len(); i++ {
		doc, err := docs.Get(i)
		fmt.Println("DOCC")
		fmt.Println(doc)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		docM := doc.(types.Document).Map()
		fmt.Println(docM)          // {map[q:{map[
		fmt.Println(docM["q"])     // {map[first:first] [first]}
		fmt.Println(docM["u"])     //{map[$set:{map[second:somesecodn] [second]}] [$set]}
		fmt.Println(docM["multi"]) // either true for updatemany or nil for updateOne

		sql := fmt.Sprintf(`select * FROM %s`, collection)
		//var placeholder pg.Placeholder

		whereSQL, args, err := whereHANA(docM["q"].(types.Document))
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		updateSQL, updateargs, err := updateMany(docM["u"].(types.Document))

		//fmt.Println(args)
		//sql += whereSQL
		//fmt.Println(sql)

		if docM["multi"] != true {
			fmt.Println("in ONE")
			sql := fmt.Sprintf("select \"_id\".\"oid\" FROM %s", collection)
			sql += whereSQL + " AND NOT (" + fmt.Sprintf(updateSQL, updateargs...) + ")" + " limit 1"
			fmt.Println("sql limit 1 in ONE")
			fmt.Println(sql)
			fmt.Println("args")
			fmt.Println(args)
			row := h.hanaPool.QueryRowContext(ctx, fmt.Sprintf(sql, args...))
			//if err != nil {
			//	fmt.Println("fail")
			//	return nil, err
			//}
			//defer row.Close()

			var objectID string

			err = row.Scan(&objectID)
			//for rows.Next() {
			//	err = rows.Scan(&objectID)
			//}
			fmt.Println("objectID")
			fmt.Println(objectID)
			countSQL := fmt.Sprintf("SELECT count(*) FROM %s", collection) + whereSQL
			fmt.Println("countSQL")
			fmt.Println(countSQL)
			countRow := h.hanaPool.QueryRowContext(ctx, fmt.Sprintf(countSQL, args...))

			//for countRow.Next() {
			//	err = countRow.Scan(&selected)
			//
			//}
			err = countRow.Scan(&selected)
			fmt.Println("selected")
			fmt.Println(selected)
			whereSQL = "WHERE \"_id\".\"oid\" = '%s'"
			var emptySlice []any
			args = append(emptySlice, objectID)
			fmt.Println("ONE args ")

		}

		//rows, err := h.hanaPool.QueryContext(ctx, fmt.Sprintf(sql, args...))
		//if err != nil {
		//	fmt.Println("fail")
		//	return nil, err
		//}

		//var updateDocs types.Array
		//
		//for {
		//	fmt.Println("hey1")
		//	updateDoc, err := nextRow(rows) // returns the row as a map
		//	fmt.Println("hey2")
		//	fmt.Println(updateDoc)
		//	if err != nil {
		//		return nil, err
		//	}
		//	if updateDoc == nil {
		//		break
		//	}
		//
		//	if err = updateDocs.Append(*updateDoc); err != nil {
		//		return nil, lazyerrors.Error(err)
		//	}
		//}

		//selected += int32(updateDocs.Len())

		sql = fmt.Sprintf("UPDATE %s SET ", collection)

		sql += fmt.Sprintf(updateSQL, updateargs...) + " " + fmt.Sprintf(whereSQL, args...)
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

		//for i := 0; i < updateDocs.Len(); i++ {
		//	updateDoc, err := updateDocs.Get(i)
		//	if err != nil {
		//		return nil, lazyerrors.Error(err)
		//	}
		//
		//	d := updateDoc.(types.Document)
		//
		//	for updateOp, updateV := range docM["u"].(types.Document).Map() {
		//		switch updateOp {
		//		case "$set":
		//			for k, v := range updateV.(types.Document).Map() {
		//				if err := d.Set(k, v); err != nil {
		//					return nil, lazyerrors.Error(err)
		//				}
		//			}
		//		default:
		//			return nil, lazyerrors.Errorf("unhandled operation %q", updateOp)
		//		}
		//	}
		//
		//	if err = updateDocs.Set(i, d); err != nil {
		//		return nil, lazyerrors.Error(err)
		//	}
		//}
		//
		//for i := 0; i < updateDocs.Len(); i++ {
		//	updateDoc, err := updateDocs.Get(i)
		//	if err != nil {
		//		return nil, lazyerrors.Error(err)
		//	}
		//
		//	sql = fmt.Sprintf("UPDATE %s SET _jsonb = $1 WHERE _jsonb->'_id' = $2", pgx.Identifier{db, collection}.Sanitize())
		//	d := updateDoc.(types.Document)
		//	db, err := bson.MustConvertDocument(d).MarshalJSON()
		//	if err != nil {
		//		return nil, err
		//	}
		//
		//	idb, err := bson.ObjectID(d.Map()["_id"].(types.ObjectID)).MarshalJSON()
		//	if err != nil {
		//		return nil, err
		//	}
		//	fmt.Println("update SQL")
		//	fmt.Println(sql)
		//	fmt.Println(db)
		//	fmt.Println(idb)
		//	tag, err := h.hanaPool.ExecContext(ctx, sql, db, idb)
		//	if err != nil {
		//		return nil, err
		//	}
		//
		//	rowsaffected, err := tag.RowsAffected()
		//
		//	updated += int32(rowsaffected)
		//}
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

	fmt.Println(updateVal.Map()["$set"])
	updateVal = updateVal.Map()["$set"].(types.Document)
	for key := range updateVal.Map() {
		fmt.Println("key:")
		fmt.Println(key)
		fmt.Println(updateVal.Map()[key])
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
		//sql += placeholder.Next()
		value, _ := updateVal.Get(key)
		fmt.Println("value")
		fmt.Println(value)
		switch value := value.(type) {
		case string:
			updateargs = append(updateargs, value)
			updateSQL += "'%s'"
		case int:
			fmt.Println("Here")
		case int64:
			fmt.Println("is Int")
			updateargs = append(updateargs, value)
		case int32:
			fmt.Println("int32")
			updateSQL += "%d"
			//newValue, errorV := strconv.ParseInt(string(value), 10, 64)
			//if errorV != nil {
			//	fmt.Println("error")
			//}
			updateargs = append(updateargs, value)
		case types.Document:
			fmt.Println("is a document")
			fmt.Println(value)
			updateSQL += "%s"
			argDoc, err := whereDocument(value)

			if err != nil {
				err = lazyerrors.Errorf("scalar: %w", err)
			}

			updateargs = append(updateargs, argDoc)
		case types.ObjectID:
			fmt.Println("is an Object")
			updateSQL += "%s"
			var bOBJ []byte
			if bOBJ, err = bson.ObjectID(value).MarshalJSONHANA(); err != nil {
				err = lazyerrors.Errorf("scalar: %w", err)
			}
			fmt.Println("bObject")
			fmt.Println(bOBJ)
			//byt := make([]byte, hex.EncodedLen(len(value[:])))
			//fmt.Println("byt")
			//fmt.Println(byt)
			//fmt.Println(string(byt))
			//bstring := "{\"oid\": " + "'" + string(byt) + "'}"
			//fmt.Println("bstring")
			//fmt.Println(bstring)
			updateargs = append(updateargs, string(bOBJ))
		default:
			fmt.Println("Nothing")
		}
	}

	return
}
