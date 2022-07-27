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

package common

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/bson"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
)

// Creates the WHERE-clause of the SQL statement.
// kvSQL stands for key-value SQL.
func Where(filter types.Document) (sql string, err error) {
	for i, key := range filter.Keys() {

		if i == 0 {
			sql += " WHERE "
		}

		value := filter.Map()[key]

		if i != 0 {
			sql += " AND "
		}
		var kvSQL string
		kvSQL, err = wherePair(key, value)

		if err != nil {
			return
		}

		sql += kvSQL
	}

	return
}

// wherePair takes a {field: value} and converts it to SQL
// vSQL: ValueSQL
// kSQL: KeySQL
func wherePair(key string, value any) (kvSQL string, err error) {
	if strings.HasPrefix(key, "$") { // {$: value}

		kvSQL, err = logicExpression(key, value)
		return

	}

	switch value := value.(type) {
	case types.Document:
		if strings.HasPrefix(value.Keys()[0], "$") { // {field: {$: value}}
			kvSQL, err = fieldExpression(key, value)
			return
		}
	}

	var vSQL string
	var sign string
	vSQL, sign, err = whereValue(value)

	if err != nil {
		return
	}

	var kSQL string
	kSQL, err = whereKey(key)
	if err != nil {
		return
	}

	kvSQL = kSQL + sign + vSQL

	if isNor {
		kvSQL = "(" + kvSQL + " AND " + kSQL + " IS SET)"
	}

	return
}

// Prepares the key (field) for SQL
func whereKey(key string) (kSQL string, err error) {
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
				kSQL += fmt.Sprintf(kIntSQL, (kInt + 1))
				isInt = true
				continue
			}

			if i != 0 {
				kSQL += "."
			}

			kSQL += "\"" + k + "\""

			isInt = false

		}
	} else {
		kSQL = "\"" + key + "\""
	}

	return
}

// Prepares the value for SQL
func whereValue(value any) (vSQL string, sign string, err error) {
	var args []any
	switch value := value.(type) {
	case int32, int64:
		vSQL = "%d"
		args = append(args, value)
	case float64:
		vSQL = "%f"
		args = append(args, value)
	case string:
		vSQL = "'%s'"
		args = append(args, value)
	case bool:
		vSQL = "to_json_boolean(%t)"
		args = append(args, value)
	case nil:
		vSQL = "NULL"
		sign = " IS "
		return
	case types.Regex:
		vSQL, err = regex(value)
		if err != nil {
			return
		}
		sign = " LIKE "
		return
	case types.ObjectID:
		var bOBJ []byte
		bOBJ, err = bson.ObjectID(value).MarshalJSON()
		if err != nil {
			return
		}
		oid := bytes.Replace(bOBJ, []byte{34}, []byte{39}, -1)
		oid = bytes.Replace(oid, []byte{39}, []byte{34}, 2)
		vSQL = "%s"
		args = append(args, string(oid))

	case types.Document:
		vSQL = "%s"
		var docValue string
		docValue, err = whereDocument(value)
		args = append(args, docValue)
	default:
		err = lazyerrors.Errorf("Value %T not supported in filter", value)
		return

	}
	sign = " = "
	vSQL = fmt.Sprintf(vSQL, args...)

	return
}

// Prepares a document for fx value = {document}.
func whereDocument(doc types.Document) (docSQL string, err error) {
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
		case types.ObjectID:
			docSQL += "%s"
			var bOBJ []byte
			bOBJ, err = bson.ObjectID(value).MarshalJSON()
			oid := bytes.Replace(bOBJ, []byte{34}, []byte{39}, -1)
			oid = bytes.Replace(oid, []byte{39}, []byte{34}, 2)
			args = append(args, string(oid))
		case *types.Array:
			var sqlArray string

			sqlArray, err = PrepareArrayForSQL(value)

			docSQL += sqlArray

		case types.Document:

			docSQL += "%s"

			var docValue string
			docValue, err = whereDocument(value)
			if err != nil {
				return
			}

			args = append(args, docValue)

		default:

			err = lazyerrors.Errorf("The document used in filter contains a datatype not yet supported: %T", value)
			return
		}
	}

	docSQL = fmt.Sprintf(docSQL, args...) + "}"

	return
}

// Needed for when an array is inside of a document used in filter
func PrepareArrayForSQL(a *types.Array) (sqlArray string, err error) {
	var value any
	var args []any
	sqlArray += "["
	for i := 0; i < a.Len(); i++ {
		if i != 0 {
			sqlArray += ", "
		}

		value, err = a.Get(i)
		if err != nil {
			return
		}

		switch value := value.(type) {
		case string, int32, int64, float64, types.ObjectID, nil:
			var sql string
			sql, _, err = whereValue(value)
			sqlArray += sql
		case *types.Array:
			var sql string
			sql, err = PrepareArrayForSQL(value)
			if err != nil {
				return
			}
			sqlArray += "%s"
			args = append(args, sql)

		case types.Document:

			sqlArray += "%s"

			var docValue string
			docValue, err = whereDocument(value)
			if err != nil {
				return
			}

			args = append(args, docValue)

		default:

			err = lazyerrors.Errorf("The document used in filter contains a datatype not yet supported: %T", value)
			return
		}
	}

	sqlArray += "]"
	sqlArray = fmt.Sprintf(sqlArray, args...)

	return
}

var (
	isNor      bool
	norCounter int
)

// Used for for example $AND and $OR
func logicExpression(key string, value any) (kvSQL string, err error) {
	logicExprMap := map[string]string{
		"$and": " AND ",
		"$or":  " OR ",
		"$nor": " AND NOT (",
	}

	lowerKey := strings.ToLower(key)

	var logicExpr string
	var ok bool
	if logicExpr, ok = logicExprMap[lowerKey]; !ok {
		err = fmt.Errorf("support for %s is not implemented yet", key)
		if strings.EqualFold(key, "$not") {
			err = fmt.Errorf("unknown top level: %s. If you are trying to negate an entire expression, use $nor", key)
		}
		return kvSQL, NewError(ErrNotImplemented, err)
	}

	var localIsNor bool
	if strings.EqualFold(key, "$nor") {
		localIsNor = true
		isNor = true
		norCounter++
	}

	kvSQL += "("

	switch value := value.(type) {
	case *types.Array:
		if value.Len() < 2 && !isNor {
			err = lazyerrors.Errorf("Need minimum two expressions")
			return
		}
		var expr any
		for i := 0; i < value.Len(); i++ {

			expr, err = value.Get(i)
			if err != nil {
				return
			}
			switch expr := expr.(type) {
			case types.Document:

				if i == 0 && localIsNor {
					kvSQL += " NOT ("
				}
				if i != 0 {
					kvSQL += logicExpr
				}

				var value any
				var exprSQL string
				for i, k := range expr.Keys() {

					if i != 0 {
						kvSQL += " AND "
					}

					value, err = expr.Get(k)
					if err != nil {
						return
					}
					exprSQL, err = wherePair(k, value)
					if err != nil {
						return
					}

					kvSQL += exprSQL

				}

			default:
				err = lazyerrors.Errorf("Found in array of logicExpression no document but instead the datatype: %T", value)
				return
			}
			if localIsNor {
				kvSQL += ")"
			}
		}

	default:
		err = lazyerrors.Errorf("Expected an array got %T", value)
		return

	}

	kvSQL += ")"

	if localIsNor {
		norCounter--
		if norCounter == 0 {
			isNor = false
		}
	}
	return
}

// Used for {field: {$: value}}
func fieldExpression(key string, value any) (kvSQL string, err error) {
	fieldExprMap := map[string]string{
		"$gt":        " > ",
		"$gte":       " >= ",
		"$lt":        " < ",
		"$lte":       " <= ",
		"$eq":        " = ",
		"$ne":        " <> ",
		"$exists":    " IS ",
		"$size":      "CARDINALITY",
		"$all":       "all",
		"$elemmatch": "elemMatch",
		"$not":       " NOT ",
		"$regex":     " LIKE ",
	}

	var kSQL string
	kSQL, err = whereKey(key)
	if err != nil {
		return
	}

	switch value := value.(type) {
	case types.Document:

		var exprValue any
		var vSQL string
		for i, k := range value.Keys() {

			if i != 0 {
				kvSQL += " AND "
			}
			kvSQL += kSQL

			lowerK := strings.ToLower(k)

			fieldExpr, ok := fieldExprMap[lowerK]
			if !ok {
				err = fmt.Errorf("support for %s is not implemented yet", k)
				return kvSQL, NewError(ErrNotImplemented, err)
			}

			exprValue, err = value.Get(k)
			if err != nil {
				return
			}
			var sign string
			if lowerK == "$exists" {
				switch exprValue := exprValue.(type) {
				case bool:
					if exprValue {
						vSQL = "SET"
					} else {
						vSQL = "UNSET"
					}
				default:
					return "", lazyerrors.Errorf("$exists only works with true or false")
				}
			} else if lowerK == "$size" {
				kvSQL = fieldExpr + "(" + kvSQL + ")"
				vSQL, fieldExpr, err = whereValue(exprValue)
				if err != nil {
					return
				}
			} else if lowerK == "$all" || lowerK == "$elemmatch" {
				kvSQL, err = filterArray(kvSQL, key, exprValue)
				if err != nil {
					return
				}
				continue
			} else if lowerK == "$not" {
				var fieldSQL string
				expr := value.Map()[k]
				fieldSQL, err = fieldExpression(key, expr)
				fieldSQL = "(" + fieldExpr + fieldSQL + " OR " + kSQL + " IS UNSET) "
				if err != nil {
					err = lazyerrors.Errorf("Wrong use of $not")
					return
				}

				kvSQL = fieldSQL
				return
			} else if lowerK == "$ne" {
				kvSQL = "(" + kvSQL
				vSQL, sign, err = whereValue(exprValue)
				if err != nil {
					return
				}
				if strings.EqualFold(sign, " IS ") {
					fieldExpr = " IS NOT "
				}

				vSQL += " OR " + kSQL + " IS UNSET)"
			} else if lowerK == "$regex" {
				vSQL, err = regex(exprValue)

			} else {
				vSQL, sign, err = whereValue(exprValue)
				if err != nil {
					return
				}

				if strings.EqualFold(sign, " IS ") {
					fieldExpr = sign
				}
			}

			kvSQL += fieldExpr + vSQL
			if isNor {
				kvSQL = "(" + kvSQL + " AND " + kSQL + " IS SET)"
			}

		}

	default:
		err = lazyerrors.Errorf("In use of field expression a document was expected. Got instead: %T", value)
	}

	return
}

// Implement $all and $elemMatch using the FOR ANY
func filterArray(field string, arrayOperator string, filters any) (kvSQL string, err error) {
	switch filters := filters.(type) {
	case types.Document:
		i := 0
		for f, v := range filters.Map() {

			if i != 0 {
				kvSQL += " AND "
			}
			var doc types.Document
			doc, err = types.MakeDocument([]any{f, v}...)
			if err != nil {
				return
			}
			var sql string
			if strings.Contains(doc.Keys()[0], "$") {
				sql, err = wherePair("element", doc)

				if strings.EqualFold(doc.Keys()[0], "$not") {
					sqlSlice := strings.Split(sql, "OR")
					sql = strings.Replace(sqlSlice[0], "(", "", 1)
				}
				if strings.Contains(sql, " IS SET") {
					sqlSlice := strings.Split(sql, " AND ")
					sql = strings.Replace(sqlSlice[0], "(", "", 1)
				}
			} else {
				var value any
				element := "element." + doc.Keys()[0]
				value, err = doc.Get(doc.Keys()[0])
				if err != nil {
					return
				}

				sql, err = wherePair(element, value)
				if _, ok := value.(types.Document); ok {
					if _, getErr := value.(types.Document).Get("$not"); getErr == nil {
						replaceIndex := strings.LastIndex(sql, "UNSET")
						sql = sql[:replaceIndex] + strings.Replace(sql[replaceIndex:], "UNSET", "NULL", 1)
					}
				}
				if strings.Contains(sql, " IS SET") {
					sqlSlice := strings.Split(sql, " AND ")
					sql = strings.Replace(sqlSlice[0], "(", "", 1)
				}
			}

			if err != nil {
				return
			}

			if i == 0 {
				kvSQL += "FOR ANY \"element\" IN " + field + " SATISFIES "
			}
			kvSQL += sql
			i++
		}

		kvSQL += " END "

	case *types.Array:
		var value string
		var v any
		for i := 0; i < filters.Len(); i++ {

			if i != 0 {
				kvSQL += " AND "
			}
			v, err = filters.Get(i)
			if err != nil {
				return
			}
			value, _, err = whereValue(v)
			if err != nil {
				return
			}
			kvSQL += "FOR ANY \"element\" IN " + field + " SATISFIES \"element\" = " + value + " END "
		}
	default:
		err = lazyerrors.Errorf("If $all: Expected array. If $elemMatch: Expected document. Got instead: %T", filters)
		return
	}

	return
}

func regex(value any) (vSQL string, err error) {

	if regex, ok := value.(types.Regex); ok {
		value = regex.Pattern
		if regex.Options != "" {
			err = lazyerrors.Errorf("The use of $options with regular expressions is not supported")
			return
		}
	}

	var escape bool
	switch value := value.(type) {
	case string:
		if strings.Contains(value, "(?i)") || strings.Contains(value, "(?-i)") {
			err = lazyerrors.Errorf("The use of (?i) and (?-i) with regular expressions is not supported")
			return
		}

		var dot bool
		for i, s := range value {
			if i == 0 {
				if s == '^' {
					continue
				}
				if s == '.' {
					dot = true
					continue
				}
				if s == '%' || s == '_' {
					vSQL += "%" + "^" + string(s)
					escape = true
					continue
				}
				vSQL += "%" + string(s)
				continue
			}

			if dot && s != '*' {
				vSQL += "%_"
				dot = false
			}

			if i == len(value)-1 {
				if s == '$' {
					continue
				}
				if s == '*' {
					vSQL += "%%"
					continue
				}
				if s == '.' {
					vSQL += "_%"
					continue
				}
				if s == '%' || s == '_' {
					vSQL += "^" + string(s) + "%"
					escape = true
					continue
				}
				vSQL += string(s) + "%"
				continue
			}

			if s == '.' {
				vSQL += "_"
				continue
			} else if s == '*' {
				vSQL += "%"
				continue
			} else if s == '%' || s == '_' {
				vSQL += "^" + string(s)
				escape = true
				continue
			}

			vSQL += string(s)

		}
	default:
		err = lazyerrors.Errorf("Expected either a JavaScript regular expression objects (i.e. /pattern/) or string containing a pattern. Got instead type %T", value)
		return
	}

	vSQL = "'" + vSQL + "'"
	if escape {
		vSQL += " ESCAPE '^' "
	}

	return
}
