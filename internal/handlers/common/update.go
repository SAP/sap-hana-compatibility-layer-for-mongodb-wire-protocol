package common

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/bson"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
)

// update creates needed SQL parts for SQL update statement
func Update(updateDoc types.Document) (updateSQL string, notWhereSQL string, err error) {
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

	if err = Unimplemented(&updateDoc, uninmplementedFields...); err != nil {
		return
	}

	updateMap := updateDoc.Map()

	var isUnsetSQL string
	var setDoc types.Document
	var ok bool
	if setDoc, ok = updateMap["$set"].(types.Document); ok {
		updateSQL, isUnsetSQL, err = createSetandUnsetSqlStmnt(setDoc, true)
		if err != nil {
			return
		}
	}

	var unSetSQL, isSetSQL string
	if unSetDoc, ok := updateMap["$unset"].(types.Document); ok {
		if unSetSQL, isSetSQL, err = createSetandUnsetSqlStmnt(unSetDoc, false); err != nil {
			return
		}
	}

	if isUnsetSQL != "" && isSetSQL != "" { // If both setting and unsetting fields
		notWhereSQL, err = CreateWhereClause(setDoc)
		if err != nil {
			if strings.Contains(err.Error(), "value *types.Array not supported in filter") {
				err = NewErrorMessage(ErrNotImplemented, "cannot update a field with array")
				return
			}
			return
		}

		notWhereSQL = " AND ( NOT ( " + strings.Replace(notWhereSQL, "WHERE", "", 1) + ") OR (" + isUnsetSQL + " ) OR ( " + isSetSQL + " ))"
		updateSQL += ", " + unSetSQL
	} else if isUnsetSQL != "" { // If only setting fields
		notWhereSQL, err = CreateWhereClause(setDoc)
		if err != nil {
			if strings.Contains(err.Error(), "value *types.Array not supported in filter") {
				err = NewErrorMessage(ErrNotImplemented, "cannot update a field with array")
				return
			}
			return
		}
		notWhereSQL = " AND ( NOT ( " + strings.Replace(notWhereSQL, "WHERE", "", 1) + ") OR (" + isUnsetSQL + " )) "
	} else if isSetSQL != "" { // If only unsetting fields
		notWhereSQL = " AND ( " + isSetSQL + " )"
		updateSQL = unSetSQL
	} else {
		err = NewErrorMessage(ErrCommandNotFound, "no such command: replaceOne")
		return
	}

	return
}

func createSetandUnsetSqlStmnt(doc types.Document, set bool) (updateSQL string, isSetOrUnsetSQL string, err error) {
	if set {
		updateSQL = " SET "
	} else {
		updateSQL = " UNSET "
	}

	var updateValue string
	for i, key := range doc.Keys() {
		var value any
		if set {
			value, _ = doc.Get(key)
		}

		if strings.EqualFold(key, "_id") {
			err = errors.New("performing an update on the path '_id' would modify the immutable field '_id'")
			return
		}

		if i != 0 {
			updateSQL += ", "
			isSetOrUnsetSQL += " OR "
		}

		var updateKey string
		updateKey, err = getUpdateKey(key)
		if err != nil {
			return
		}

		if set {
			updateValue, err = GetUpdateValue(value)
			if err != nil {
				return
			}
			updateSQL += updateKey + " = " + updateValue
			isSetOrUnsetSQL += updateKey + " IS UNSET"
		} else {
			updateSQL += updateKey
			isSetOrUnsetSQL += updateKey + " IS SET"
		}
	}
	return
}

// getUpdateKey prepares the key (field) for SQL statement
func getUpdateKey(key string) (updateKey string, err error) {
	if strings.Contains(key, ".") {
		splitKey := strings.Split(key, ".")

		var isInt bool
		for i, k := range splitKey {

			if kInt, convErr := strconv.Atoi(k); convErr == nil {
				if isInt {
					err = NewErrorMessage(ErrNotImplemented, "not yet supporting indexing on an array inside of an array")
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

// getUpdateValue prepares the value for SQL statement
func GetUpdateValue(value any) (updateValue string, err error) {
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
		updateValue, err = PrepareArrayForSQL(value)
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

// updateDocument prepares a document for being used as value for updating a field
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
			arraySQL, err = PrepareArrayForSQL(value)
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
			err = NewErrorMessage(ErrBadValue, "%T is not supported within an object for filtering", value)
			return
		}
	}

	docSQL = fmt.Sprintf(docSQL, args...) + "}"
	return
}
