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
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/bson"
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

		whereSQL, err := common.Where(docM["q"].(types.Document))
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
		// notWhereSQL makes sure we do not update documents which do not need an update
		updateSQL, notWhereSQL, err := update(docM["u"].(types.Document))
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		// Get amount of documents that fits the filter. MatchCount
		countSQL := fmt.Sprintf("SELECT count(*) FROM %s.%s", db, collection) + whereSQL
		countRow := h.hanaPool.QueryRowContext(ctx, countSQL)

		err = countRow.Scan(&matched)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		var args []any
		if docM["multi"] != true { // If updateOne()

			// We get the _id of the one document to update.
			sql := fmt.Sprintf("SELECT {\"_id\": \"_id\"} FROM %s.%s", db, collection)
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

			updateId, err := getUpdateValue(id.(types.Document).Map()["_id"])
			if err != nil {
				return nil, err
			}

			whereSQL = "WHERE \"_id\" = %s"
			var emptySlice []any
			args = append(emptySlice, updateId)
			notWhereSQL = ""
		}

		sql := fmt.Sprintf("UPDATE %s.%s ", db, collection)

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

// Creates needed SQL parts for SQL update statement
func update(updateDoc types.Document) (updateSQL string, notWhereSQL string, err error) {
	uninmplementedFields := []string{
		"$currentDate",
		"$inc",
		"$min",
		"$max",
		"$mul",
		"$rename",
		"$setOnInsert",
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
		"$replaceRoot",
		"$replaceWith",
	}

	if err = common.Unimplemented(&updateDoc, uninmplementedFields...); err != nil {
		return
	}

	updateMap := updateDoc.Map()

	var isUnsetSQL string
	var setDoc types.Document
	var ok bool
	if setDoc, ok = updateMap["$set"].(types.Document); ok {
		updateSQL, isUnsetSQL, err = setFields(setDoc)
		if err != nil {
			return
		}
	}

	var unSetSQL, isSetSQL string
	if unSetDoc, ok := updateMap["$unset"].(types.Document); ok {
		if unSetSQL, isSetSQL, err = unsetFields(unSetDoc); err != nil {
			return
		}
	}

	if isUnsetSQL != "" && isSetSQL != "" { // If both setting and unsetting fields
		notWhereSQL, err = common.Where(setDoc)
		if err != nil {
			if strings.Contains(err.Error(), "Value *types.Array not supported in filter") {
				err = lazyerrors.Errorf("Cannot update field with array")
				return
			}
			return
		}

		notWhereSQL = " AND ( NOT ( " + strings.Replace(notWhereSQL, "WHERE", "", 1) + ") OR (" + isUnsetSQL + " ) OR ( " + isSetSQL + " ))"
		updateSQL += ", " + unSetSQL
	} else if isUnsetSQL != "" { // If only unsetting fields
		notWhereSQL, err = common.Where(setDoc)
		if err != nil {
			if strings.Contains(err.Error(), "Value *types.Array not supported in filter") {
				err = lazyerrors.Errorf("Cannot update field with array")
				return
			}
			return
		}
		notWhereSQL = " AND ( NOT ( " + strings.Replace(notWhereSQL, "WHERE", "", 1) + ") OR (" + isUnsetSQL + " )) "
	} else if isSetSQL != "" { // If only setting fields
		notWhereSQL = " AND ( " + isSetSQL + " )"
		updateSQL = unSetSQL
	} else {
		err = common.NewErrorMessage(common.ErrCommandNotFound, "no such command: replaceOne")
		return
	}

	return
}

// Create SQL for setting fields
func setFields(setDoc types.Document) (updateSQL string, isUnsetSQL string, err error) {
	updateSQL = " SET "

	var updateValue string
	i := 0
	for key, value := range setDoc.Map() {

		if strings.EqualFold(key, "_id") {
			err = errors.New("performing an update on the path '_id' would modify the immutable field '_id'")
			return
		}

		if i != 0 {
			updateSQL += ", "
			isUnsetSQL += " OR "
		}

		var updateKey string
		updateKey, err = getUpdateKey(key)
		if err != nil {
			return
		}

		updateValue, err = getUpdateValue(value)
		if err != nil {
			return
		}

		updateSQL += updateKey + " = " + updateValue
		isUnsetSQL += updateKey + " IS UNSET"
		i++
	}

	return
}

// Create SQL for unsetting fields
func unsetFields(unSetDoc types.Document) (unsetSQL string, isSetSQL string, err error) {
	unsetSQL = " UNSET "

	i := 0
	for key := range unSetDoc.Map() {

		if strings.EqualFold(key, "_id") {
			err = errors.New("performing an update on the path '_id' would modify the immutable field '_id'")
			return
		}

		if i != 0 {
			unsetSQL += ", "
			isSetSQL += " OR "
		}

		var updateKey string
		updateKey, err = getUpdateKey(key)
		if err != nil {
			return
		}

		unsetSQL += updateKey

		isSetSQL += updateKey + " IS SET"

	}

	return
}

// Prepares the key (field) for SQL statement
func getUpdateKey(key string) (updateKey string, err error) {
	if strings.Contains(key, ".") {
		splitKey := strings.Split(key, ".")

		var isInt bool
		for i, k := range splitKey {

			if kInt, convErr := strconv.Atoi(k); convErr == nil {
				if isInt {
					err = lazyerrors.Errorf("Not allowed to index on an array inside of an array.")
					return
				}
				kIntSQL := "[" + "%d" + "]"
				updateKey += fmt.Sprintf(kIntSQL, (kInt + 1))
				isInt = true
				continue
			}

			if i != 0 {
				updateKey += "."
			}

			updateKey += "\"" + k + "\""

			isInt = false

		}
	} else {
		updateKey = "\"" + key + "\""
	}

	return
}

// Prepares the value for SQL statement
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
	case bool:
		updateValue += "to_json_boolean(%t)"
		updateArgs = append(updateArgs, value)
	case *types.Array:
		updateValue, err = common.PrepareArrayForSQL(value)
		if err != nil {
			return
		}
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

// Prepares a document for being used as value for updating a field
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

			docSQL += "to_json_boolean(%t)"
			args = append(args, value)
		case nil:
			docSQL += " NULL "
		case *types.Array:
			var arraySQL string
			arraySQL, err = common.PrepareArrayForSQL(value)
			docSQL += arraySQL
			if err != nil {
				return
			}
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

			err = lazyerrors.Errorf("whereDocument does not support datatype %T, yet.", value)
			return
		}
	}

	docSQL = fmt.Sprintf(docSQL, args...) + "}"
	return
}
