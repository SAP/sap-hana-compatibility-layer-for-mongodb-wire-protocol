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

	"github.com/DocStore/HANA_HWY/internal/bson"

	"github.com/DocStore/HANA_HWY/internal/types"
	"github.com/DocStore/HANA_HWY/internal/util/lazyerrors"
)

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

func wherePair(key string, value any) (kvSQL string, err error) {
	if strings.HasPrefix(key, "$") {

		kvSQL, err = logicExpression(key, value)
		return

	}

	switch value := value.(type) {
	case types.Document:
		if strings.HasPrefix(value.Keys()[0], "$") {
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

	kSQL := whereKey(key)
	kvSQL = kSQL + sign + vSQL

	return
}

func whereKey(key string) (kSQL string) {
	if strings.Contains(key, ".") {
		splitKey := strings.Split(key, ".")
		for i, k := range splitKey {

			if kInt, err := strconv.Atoi(k); err == nil {
				kIntSQL := "[" + "%d" + "]"
				kSQL += fmt.Sprintf(kIntSQL, (kInt + 1))
				continue
			}

			if i != 0 {
				kSQL += "."
			}

			kSQL += "\"" + k + "\""

		}
	} else {
		kSQL = "\"" + key + "\""
	}

	return
}

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
		err = lazyerrors.Errorf("Value for WHERE not fitting any supported datatypes.")
		return

	}
	sign = " = "
	vSQL = fmt.Sprintf(vSQL, args...)

	return
}

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
			docValue, err = whereDocument(value)
			if err != nil {
				return
			}

			args = append(args, docValue)

		default:

			err = lazyerrors.Errorf("whereDocument does not support this datatype, yet. And it is %T", value)
			return
		}
	}

	docSQL = fmt.Sprintf(docSQL, args...) + "}"

	return
}

func logicExpression(key string, value any) (kvSQL string, err error) {
	logicExprMap := map[string]string{
		"$AND": " AND ",
		"$OR":  " OR ",
	}

	if _, ok := logicExprMap[key]; !ok {
		err = fmt.Errorf("support for %s is not implemented yet", key)
		return kvSQL, NewError(ErrNotImplemented, err)
	}

	kvSQL += "("

	switch value := value.(type) {
	case *types.Array:
		if value.Len() < 2 {
			err = lazyerrors.Errorf("Need minimum to expressions")
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

				if i != 0 {
					kvSQL += logicExprMap[key]
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
				err = lazyerrors.Errorf("Found in array of logicExpression no document but instead %T", value)
				return
			}

		}

	default:
		err = lazyerrors.Errorf("Found in array of logicExpression no document but instead %T", value)
		return

	}

	kvSQL += ")"

	return
}

func fieldExpression(key string, value any) (kvSQL string, err error) {
	fieldExprMap := map[string]string{
		"$gt":     " > ",
		"$gte":    " >= ",
		"$lt":     " < ",
		"$lte":    " <= ",
		"$eq":     "=",
		"$ne":     "<>",
		"$exists": "IS",
		"$size":   "CARDINALITY",
		"$all":    "all",
	}

	kvSQL += whereKey(key)

	switch value := value.(type) {
	case types.Document:

		var exprValue any
		var vSQL string
		for i, k := range value.Keys() {
			if i == 1 {
				err = lazyerrors.Errorf("Only one expression allowed")
				return
			}

			fieldExpr, ok := fieldExprMap[k]
			if !ok {
				err = fmt.Errorf("support for %s is not implemented yet", k)
				return kvSQL, NewError(ErrNotImplemented, err)
			}

			exprValue, err = value.Get(k)
			if err != nil {
				return
			}

			if k == "$exists" {
				switch exprValue := exprValue.(type) {
				case bool:
					if exprValue {
						vSQL = " SET"
					} else {
						vSQL = " UNSET"
					}
				default:
					return "", lazyerrors.Errorf("$exists only works with true or false")
				}
			} else if k == "$size" {
				kvSQL = fieldExpr + "(" + kvSQL + ")"
				vSQL, fieldExpr, err = whereValue(exprValue)
				if err != nil {
					return
				}
			} else if k == "$all" {

				kvSQL, err = filterArray(kvSQL, key, exprValue)
				if err != nil {
					return
				}
				continue

			} else {
				vSQL, _, err = whereValue(exprValue)
				if err != nil {
					return
				}
			}

			kvSQL += fieldExpr + vSQL

		}

	default:
		err = lazyerrors.Errorf("wrong use of filter")
	}

	return
}

func filterArray(field string, arrayOperator string, valueArray any) (kvSQL string, err error) {

	switch valueArray := valueArray.(type) {
	case types.Document:
		fmt.Println("doc")
	case *types.Array:
		var value string
		var v any
		for i := 0; i < valueArray.Len(); i++ {

			if i != 0 {
				kvSQL += " AND "
			}
			v, err = valueArray.Get(i)
			if err != nil {
				return
			}
			value, _, err = whereValue(v)
			if err != nil {
				return
			}
			kvSQL += "FOR ANY \"element\" IN " + field + " SATISFIES \"element\" = " + value + " END "
		}
	}

	return
}
