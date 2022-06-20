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
	"strings"

	"github.com/DocStore/HANA_HWY/internal/bson"

	"github.com/DocStore/HANA_HWY/internal/types"
	"github.com/DocStore/HANA_HWY/internal/util/lazyerrors"
)

// func scalar(v any, p *pg.Placeholder) (sql string, args []any, err error) {
// 	var arg any
// 	switch v := v.(type) {
// 	case int32:
// 		sql = "to_jsonb(" + p.Next() + "::int4)"
// 		arg = v
// 	case string:
// 		sql = "to_jsonb(" + p.Next() + "::text)"
// 		arg = v
// 	case types.ObjectID:
// 		sql = p.Next()
// 		var b []byte
// 		if b, err = bson.ObjectID(v).MarshalJSON(); err != nil {
// 			err = lazyerrors.Errorf("scalar: %w", err)
// 			return
// 		}
// 		arg = string(b)
// 	case types.Regex:
// 		var options string
// 		for _, o := range v.Options {
// 			switch o {
// 			case 'i':
// 				options += "i"
// 			default:
// 				err = lazyerrors.Errorf("scalar: unhandled regex option %v (%v)", o, v)
// 			}
// 		}
// 		sql = p.Next()
// 		arg = v.Pattern
// 		if options != "" {
// 			arg = "(?" + options + ")" + v.Pattern
// 		}
// 	default:
// 		err = lazyerrors.Errorf("scalar: unhandled field %v (%T)", v, v)
// 	}

// 	args = []any{arg}
// 	return
// }

// // fieldExpr handles {field: {expr}}.
// func fieldExpr(field string, expr types.Document, p *pg.Placeholder) (sql string, args []any, err error) {
// 	filterKeys := expr.Keys()
// 	filterMap := expr.Map()

// 	for _, op := range filterKeys {
// 		if op == "$options" {
// 			// handled by $regex, no need to modify sql in any way
// 			continue
// 		}

// 		if sql != "" {
// 			sql += " AND"
// 		}

// 		var argSql string
// 		var arg []any
// 		value := filterMap[op]

// 		// {field: {$not: {expr}}}
// 		if op == "$not" {
// 			if sql != "" {
// 				sql += " "
// 			}
// 			sql += "NOT("

// 			argSql, arg, err = fieldExpr(field, value.(types.Document), p)
// 			if err != nil {
// 				err = lazyerrors.Errorf("fieldExpr: %w", err)
// 				return
// 			}

// 			sql += argSql + ")"
// 			args = append(args, arg...)

// 			continue
// 		}

// 		if sql != "" {
// 			sql += " "
// 		}
// 		args = append(args, field)

// 		switch op {
// 		case "$in":
// 			// {field: {$in: [value1, value2, ...]}}
// 			sql += "_jsonb->" + p.Next() + " IN"
// 			argSql, arg, err = common.InArray(value.(*types.Array), p, scalar)
// 		case "$nin":
// 			// {field: {$nin: [value1, value2, ...]}}
// 			sql += "_jsonb->" + p.Next() + " NOT IN"
// 			argSql, arg, err = common.InArray(value.(*types.Array), p, scalar)
// 		case "$eq":
// 			// {field: {$eq: value}}
// 			// TODO special handling for regex
// 			sql += "_jsonb->" + p.Next() + " ="
// 			argSql, arg, err = scalar(value, p)
// 		case "$ne":
// 			// {field: {$ne: value}}
// 			sql += "_jsonb->" + p.Next() + " <>"
// 			argSql, arg, err = scalar(value, p)
// 		case "$lt":
// 			// {field: {$lt: value}}
// 			sql += "_jsonb->" + p.Next() + " <"
// 			argSql, arg, err = scalar(value, p)
// 		case "$lte":
// 			// {field: {$lte: value}}
// 			sql += "_jsonb->" + p.Next() + " <="
// 			argSql, arg, err = scalar(value, p)
// 		case "$gt":
// 			// {field: {$gt: value}}
// 			sql += "_jsonb->" + p.Next() + " >"
// 			argSql, arg, err = scalar(value, p)
// 		case "$gte":
// 			// {field: {$gte: value}}
// 			sql += "_jsonb->" + p.Next() + " >="
// 			argSql, arg, err = scalar(value, p)
// 		case "$regex":
// 			// {field: {$regex: value}}

// 			var options string
// 			if opts, ok := filterMap["$options"]; ok {
// 				// {field: {$regex: value, $options: string}}
// 				if options, ok = opts.(string); !ok {
// 					err = common.NewErrorMessage(common.ErrBadValue, "$options has to be a string")
// 					return
// 				}
// 			}

// 			sql += "_jsonb->>" + p.Next() + " ~"
// 			switch value := value.(type) {
// 			case string:
// 				// {field: {$regex: string}}
// 				v := types.Regex{
// 					Pattern: value,
// 					Options: options,
// 				}
// 				argSql, arg, err = scalar(v, p)
// 			case types.Regex:
// 				// {field: {$regex: /regex/}}
// 				if options != "" {
// 					if value.Options != "" {
// 						err = common.NewErrorMessage(common.ErrRegexOptions, "options set in both $regex and $options")
// 						return
// 					}
// 					value.Options = options
// 				}
// 				argSql, arg, err = scalar(value, p)
// 			default:
// 				err = common.NewErrorMessage(common.ErrBadValue, "$regex has to be a string")
// 				return
// 			}
// 		default:
// 			err = lazyerrors.Errorf("unhandled {%q: %v}", op, value)
// 		}

// 		if err != nil {
// 			err = lazyerrors.Errorf("fieldExpr: %w", err)
// 			return
// 		}

// 		sql += " " + argSql
// 		args = append(args, arg...)
// 	}

// 	return
// }

// func wherePair(key string, value any, p *pg.Placeholder) (sql string, args []any, err error) {
// 	if strings.HasPrefix(key, "$") {
// 		exprs := value.(*types.Array)
// 		sql, args, err = common.LogicExpr(key, exprs, p, wherePair)
// 		return
// 	}

// 	switch value := value.(type) {
// 	case types.Document:
// 		// {field: {expr}}
// 		sql, args, err = fieldExpr(key, value, p)

// 	default:
// 		// {field: value}
// 		switch value.(type) {
// 		case types.Regex:
// 			sql = "_jsonb->>" + p.Next() + " ~ "
// 		default:
// 			sql = "_jsonb->" + p.Next() + " = "
// 		}

// 		args = append(args, key)

// 		var scalarSQL string
// 		var scalarArgs []any
// 		scalarSQL, scalarArgs, err = scalar(value, p)
// 		sql += scalarSQL
// 		args = append(args, scalarArgs...)
// 	}

// 	if err != nil {
// 		err = lazyerrors.Errorf("wherePair: %w", err)
// 	}

// 	return
// }

// func where(filter types.Document, p *pg.Placeholder) (sql string, args []any, err error) {
// 	filterMap := filter.Map()
// 	if len(filterMap) == 0 {
// 		return
// 	}

// 	sql = " WHERE"

// 	for i, key := range filter.Keys() {
// 		value := filterMap[key]

// 		if i != 0 {
// 			sql += " AND"
// 		}

// 		var argSql string
// 		var arg []any
// 		argSql, arg, err = wherePair(key, value, p)
// 		if err != nil {
// 			err = lazyerrors.Errorf("where: %w", err)
// 			return
// 		}

// 		sql += " (" + argSql + ")"
// 		args = append(args, arg...)
// 	}

// 	return
// }

func WhereDocument1(document types.Document) (sql string, err error) {
	var args []any
	sqlKeys := "{\"keys\": ["
	count := 0

	for key := range document.Map() {

		if count != 0 && (len(document.Map())-1) == count {
			sql += ","
			sqlKeys += ","
		}
		sqlKeys += "'" + key + "'"
		count += 1

		sql += "\"" + key + "\":"

		value, _ := document.Get(key)

		switch value := value.(type) {
		case string:
			args = append(args, value)
			sql += "'%s'"
		case int64:
			args = append(args, value)
		case int32:
			sql += "%d"
			args = append(args, value)
		case float64:
			sql += "%f"
			args = append(args, value)
		case types.ObjectID:
			sql += "%s"
			var bOBJ []byte
			var err1 error
			if bOBJ, err1 = bson.ObjectID(value).MarshalJSON(); err != nil {
				err = err1
				return
			}
			args = append(args, string(bOBJ))
		default:
			err = lazyerrors.Errorf("scalar did not fit cases ")
			return
		}

	}
	sqlKeys += "],"
	sqlnew := fmt.Sprintf(sql, args...)
	sqlnew += "}"
	sql = sqlKeys + sqlnew

	return
}

func WhereHANA(filter types.Document) (sql string, args []any, err error) {
	unimplementedFields := []string{
		"$eq",
		"$gt",
		"$gte",
		"$in",
		"$lt",
		"$lte",
		"$ne",
		"$nin",
		"$and",
		"$not",
		"$nor",
		"$or",
		"$exists",
		"$type",
		"$expr",
		"$jsonSchema",
		"$mod",
		"$regex",
		"$text",
		"$where",
		"$geoIntersects",
		"$geoWithin",
		"$near",
		"$nearSphere",
		"$all",
		"$elemMatch",
		"$size",
		"$bitsAllClear",
		"$bitsAllSet",
		"$bitsAnyClear",
		"$bitsAnySet",
		"$",
		"$elemMatch",
		"$meta",
		"$slice",
		"$comment",
		"$rand",
	}

	if err := Unimplemented(&filter, unimplementedFields...); err != nil {
		return "", nil, err
	}

	for key := range filter.Map() {
		sql += " WHERE "
		if strings.Contains(key, ".") {
			split := strings.Split(key, ".")
			count := 0
			for _, s := range split {
				if (len(split) - 1) == count {
					sql += "\"" + s + "\""
				} else {
					sql += "\"" + s + "\"."
				}
				count += 1
			}
		} else {
			sql += "\"" + key + "\""
		}

		sql += " = "
		value, _ := filter.Get(key)

		switch value := value.(type) {
		case string:
			args = append(args, value)
			sql += "'%s'"
		case int64:
			args = append(args, value)
		case int32:
			sql += "%d"
			args = append(args, value)
		case types.Document:
			sql += "%s"
			argDoc, err1 := WhereDocument1(value)

			if err1 != nil {
				err = lazyerrors.Errorf("scalar: %w", err1)
				return
			}

			args = append(args, argDoc)
		case types.ObjectID:
			sql += "%s"
			var bOBJ []byte
			if bOBJ, err = bson.ObjectID(value).MarshalJSON(); err != nil {
				err = lazyerrors.Errorf("scalar: %w", err)
			}

			oid := bytes.Replace(bOBJ, []byte{34}, []byte{39}, -1)
			oid = bytes.Replace(oid, []byte{39}, []byte{34}, 2)
			args = append(args, string(oid))
		default:
			err = lazyerrors.Errorf("Scalar did not fit cases")
			return
		}
	}

	return
}

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
	vSQL, err = whereValue(value)

	if err != nil {
		return
	}

	kSQL := whereKey(key)
	kvSQL = kSQL + " = " + vSQL

	return
}

func whereKey(key string) (kSQL string) {
	if strings.Contains(key, ".") {
		splitKey := strings.Split(key, ".")
		for i, k := range splitKey {

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

func whereValue(value any) (vSQL string, err error) {
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
		case types.Document:

			docSQL += "%s"

			var docValue string
			docValue, err = whereDocument(value)
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
		"$gt":  " > ",
		"$gte": " >= ",
		"$lt":  " < ",
		"$lte": " <= ",
		"$eq":  "=",
		"$ne":  "<>",
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
			vSQL, err = whereValue(exprValue)
			if err != nil {
				return
			}

			kvSQL += fieldExpr + vSQL

		}

	default:
		err = lazyerrors.Errorf("wrong use of filter")
	}

	return
}
