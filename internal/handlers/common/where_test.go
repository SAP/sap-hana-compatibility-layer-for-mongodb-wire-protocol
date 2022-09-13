// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"fmt"
	"strings"
	"testing"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
)

type testCaseWhere struct {
	name string
	r    types.Document
	e    expectedWhereKey
}

func TestWhere(t *testing.T) {
	whereTestCases := []testCaseWhere{
		{name: "where equal test", r: types.MustMakeDocument("equal_string", "string",
			"equal_int32", int32(1),
			"equal_int64", int64(123123123123),
			"equal_bool", true,
			"equal_eq", types.MustMakeDocument("$eq", "equal"),
			"equal_document", types.MustMakeDocument("field", int32(123)),
			"equal_float64", float64(123.123),
			"equal_objId", types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107},
		), e: expectedWhereKey{sql: " WHERE \"equal_string\" = 'string' AND \"equal_int32\" = 1 AND \"equal_int64\" = 123123123123 AND \"equal_bool\" = to_json_boolean(true) AND " +
			"\"equal_eq\" = 'equal' AND \"equal_document\" = {\"field\": 123} AND \"equal_float64\" = 123.123000 AND \"equal_objId\" = {\"oid\":'62e2bd54510683f9c0bb0d6b'}", err: nil}},
		{name: "where comparison test", r: types.MustMakeDocument("greaterThan_int32", types.MustMakeDocument("$gt", int32(12)),
			"lessThan_int64", types.MustMakeDocument("$lt", int64(123123)),
		), e: expectedWhereKey{sql: " WHERE \"greaterThan_int32\" > 12 AND \"lessThan_int64\" < 123123", err: nil}},
		{
			name: "logic expression test", r: types.MustMakeDocument("$or", types.MustNewArray(types.MustMakeDocument("field", "new"), types.MustMakeDocument("field2", true))),
			e: expectedWhereKey{sql: " WHERE (\"field\" = 'new' OR \"field2\" = to_json_boolean(true))", err: nil},
		},
		{
			name: "double array index error", r: types.MustMakeDocument("array.1.2", int32(1)),
			e: expectedWhereKey{sql: " WHERE ", err: fmt.Errorf("Not allowed to index on an array inside of an array")},
		},
		{
			name: "double array index error", r: types.MustMakeDocument("array.1", types.MustNewArray(int32(32))),
			e: expectedWhereKey{sql: " WHERE ", err: fmt.Errorf("Value *types.Array not supported in filter")},
		},
	}

	for _, field := range whereTestCases {

		sql, err := CreateWhereClause(field.r)

		if field.e.err != nil {
			if !strings.EqualFold(sql, field.e.sql) || !strings.Contains(err.Error(), field.e.err.Error()) {
				t.Errorf("%s: where(%v) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.err, sql, err)
			}
		} else {
			if !strings.EqualFold(sql, field.e.sql) || err != field.e.err {
				t.Errorf("%s: where(%v) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.err, sql, err)
			}
		}

	}
}

type testCaseWhereKey struct {
	name string
	r    string
	e    expectedWhereKey
}

type expectedWhereKey struct {
	sql  string
	sign string
	err  error
}

func TestWhereKey(t *testing.T) {
	whereKeyTestCases := []testCaseWhereKey{
		{name: "singe field test", r: "oneField", e: expectedWhereKey{sql: "\"oneField\"", err: nil}},
		{name: "multiple fields test", r: "oneField.twoField.threeField", e: expectedWhereKey{sql: "\"oneField\".\"twoField\".\"threeField\"", err: nil}},
		{name: "field with array index test", r: "array.0", e: expectedWhereKey{sql: "\"array\"[1]", err: nil}},
		{name: "mix multiple fields and index test", r: "oneField.array.0.twoField", e: expectedWhereKey{sql: "\"oneField\".\"array\"[1].\"twoField\"", err: nil}},
		{name: "field with negative array index error test", r: "array.-1", e: expectedWhereKey{sql: "", err: fmt.Errorf("Negative array index")}},
		{name: "double array index error test", r: "array.0.1", e: expectedWhereKey{sql: "", err: fmt.Errorf("Not allowed to index on an array")}},
	}

	for _, field := range whereKeyTestCases {

		sql, err := whereKey(field.r)

		if field.e.err != nil {
			if !strings.EqualFold(sql, field.e.sql) || !strings.Contains(err.Error(), field.e.err.Error()) {
				t.Errorf("%s: whereKey(%s) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.err, sql, err)
			}
		} else {
			if !strings.EqualFold(sql, field.e.sql) || err != field.e.err {
				t.Errorf("%s: whereKey(%s) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.err, sql, err)
			}
		}

	}
}

type testCaseWhereValue struct {
	name string
	r    any
	e    expectedWhereKey
}

func TestWhereValue(t *testing.T) {
	whereValueTestCases := []testCaseWhereValue{
		{name: "string test", r: "string", e: expectedWhereKey{sql: "'string'", sign: " = ", err: nil}},
		{name: "int32 test", r: int32(123), e: expectedWhereKey{sql: "123", sign: " = ", err: nil}},
		{name: "int32 test", r: int64(123), e: expectedWhereKey{sql: "123", sign: " = ", err: nil}},
		{name: "float64 test", r: float64(123.123), e: expectedWhereKey{sql: "123.123000", sign: " = ", err: nil}},
		{name: "boolean test", r: true, e: expectedWhereKey{sql: "to_json_boolean(true)", sign: " = ", err: nil}},
		{name: "boolean test", r: true, e: expectedWhereKey{sql: "to_json_boolean(true)", sign: " = ", err: nil}},
		{name: "nil test", r: nil, e: expectedWhereKey{sql: "NULL", sign: " IS ", err: nil}},
		{name: "regex no begin and end sign test", r: types.Regex{Pattern: "pattern"}, e: expectedWhereKey{sql: "'%Pattern%'", sign: " LIKE ", err: nil}},
		{name: "regex with begin and end sign test", r: types.Regex{Pattern: "^pattern$"}, e: expectedWhereKey{sql: "'Pattern'", sign: " LIKE ", err: nil}},
		{name: "regex with begin and end sign test", r: types.Regex{Pattern: "^pa_tt_ern$"}, e: expectedWhereKey{sql: "'Pa^_tt^_ern' ESCAPE '^' ", sign: " LIKE ", err: nil}},
		{name: "regex everything test", r: types.Regex{Pattern: "^pa_t.t_er.*n$"}, e: expectedWhereKey{sql: "'Pa^_t_t^_er%n' ESCAPE '^' ", sign: " LIKE ", err: nil}},
		{name: "regex many dots at beginning test", r: types.Regex{Pattern: "...pa_t.t_er.*n$"}, e: expectedWhereKey{sql: "'%___Pa^_t_t^_er%n' ESCAPE '^' ", sign: " LIKE ", err: nil}},
		{name: "regex many dots at end test", r: types.Regex{Pattern: "pa_t.t_er.*n..."}, e: expectedWhereKey{sql: "'%Pa^_t_t^_er%n___%' ESCAPE '^' ", sign: " LIKE ", err: nil}},
		{name: "regex many dots in middle test", r: types.Regex{Pattern: "pa_t...t_er.*n"}, e: expectedWhereKey{sql: "'%Pa^_t___t^_er%n%' ESCAPE '^' ", sign: " LIKE ", err: nil}},
		{name: "regex use of escape at begin and end test", r: types.Regex{Pattern: "_pa_t...t_er.*n%"}, e: expectedWhereKey{sql: "'%^_Pa^_t___t^_er%n^%%' ESCAPE '^' ", sign: " LIKE ", err: nil}},
		{name: "regex option error test", r: types.Regex{Pattern: "_pa_t...t_er.*n%", Options: "m"}, e: expectedWhereKey{sql: "", sign: "", err: fmt.Errorf("The use of $options with regular expressions is not supported")}},
		{name: "regex (i?) error test", r: types.Regex{Pattern: "patt(?i)ern"}, e: expectedWhereKey{sql: "", sign: "", err: fmt.Errorf("The use of (?i) and (?-i) with regular expressions is not supported")}},
		{name: "regex (?-i) error test", r: types.Regex{Pattern: "pat(?-i)tern"}, e: expectedWhereKey{sql: "", sign: "", err: fmt.Errorf("The use of (?i) and (?-i) with regular expressions is not supported")}},
		{name: "ObjectID test", r: types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107}, e: expectedWhereKey{sql: "{\"oid\":'62e2bd54510683f9c0bb0d6b'}", sign: " = ", err: nil}},
		{
			name: "document test", r: types.MustMakeDocument(
				"bool", true,
				"int32", int32(0),
				"int64", int64(223372036854775807),
				"objectID", types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107},
				"string", "foo",
				"null", nil),
			e: expectedWhereKey{sql: "{\"bool\": to_json_boolean(true), \"int32\": 0, \"int64\": 223372036854775807, \"objectID\": {\"oid\":'62e2bd54510683f9c0bb0d6b'}, \"string\": 'foo', \"null\":  NULL }", sign: " = ", err: nil},
		},
		{name: "type error test", r: int(34), e: expectedWhereKey{sql: "", sign: "", err: fmt.Errorf("Value int not supported in filter")}},
	}

	for _, field := range whereValueTestCases {

		sql, sign, err := whereValue(field.r)

		if field.e.err != nil {
			if !strings.EqualFold(sql, field.e.sql) || !strings.Contains(err.Error(), field.e.err.Error()) || !strings.EqualFold(sign, field.e.sign) {
				t.Errorf("%s: whereKey(%v) FAILED. Expected sql = %v, sign = %s and err = %v got sql = %s, sign = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.sign, field.e.err, sql, sign, err)
			}
		} else {
			if !strings.EqualFold(sql, field.e.sql) || err != field.e.err || !strings.EqualFold(sign, field.e.sign) {
				t.Errorf("%s: whereKey(%v) FAILED. Expected sql = %v, sign = %s and err = %v got sql = %s, sign = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.sign, field.e.err, sql, sign, err)
			}
		}

	}
}

func TestWhereDocument(t *testing.T) {
	whereDocumentTestCases := []testCaseWhere{
		{
			name: "test document all data types", r: types.MustMakeDocument("int32", int32(0), "int64", int64(9090123123), "float64", float64(898.341123),
				"string", "normal string", "bool", true, "nil", nil, "objID", types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107},
				"array", types.MustNewArray(int32(543), "string"), "document", types.MustMakeDocument("field", "name", "bool", true)),
			e: expectedWhereKey{sql: "{\"int32\": 0, \"int64\": 9090123123, \"float64\": 898.341123, \"string\": 'normal string', \"bool\": to_json_boolean(true), \"nil\":  NULL , \"objID\": {\"oid\":'62e2bd54510683f9c0bb0d6b'}, \"array\": [543, 'string'], \"document\": {\"field\": 'name', \"bool\": to_json_boolean(true)}}", err: nil},
		},
		{
			name: "not supported datatype test", r: types.MustMakeDocument("binary", types.Binary{Subtype: types.BinarySubtype(byte(12)), B: []byte("hello")}),
			e: expectedWhereKey{sql: "{\"binary\": ", err: fmt.Errorf("The document used in filter contains a datatype not yet supported: types.Binary")},
		},
	}

	for _, field := range whereDocumentTestCases {
		docSQL, err := whereDocument(field.r)

		if field.e.err != nil {
			if !strings.EqualFold(docSQL, field.e.sql) || !strings.Contains(err.Error(), field.e.err.Error()) {
				t.Errorf("%s: whereKey(%v) FAILED. Expected sql = %s and err = %v got sql = %s, sign = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.sign, field.e.err, docSQL, err)
			}
		} else {
			if !strings.EqualFold(docSQL, field.e.sql) || err != field.e.err {
				t.Errorf("%s: whereKey(%v) FAILED. Expected sql = %s and err = %v got sql = %s, sign = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.sign, field.e.err, docSQL, err)
			}
		}

	}
}

type testCasePrepareArraySQL struct {
	name string
	r    *types.Array
	e    expectedWhereKey
}

func TestPrepareArraySQL(t *testing.T) {
	prepareArrayForSQLTestCases := []testCasePrepareArraySQL{
		{
			name: "all datatypes", r: types.MustNewArray(int32(12), int64(123123), "string", float64(321.321), types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107}, nil, types.MustMakeDocument("field", int32(123)), false, types.MustNewArray(int32(123), "new_array")),
			e: expectedWhereKey{sql: "[12, 123123, 'string', 321.321000, {\"oid\":'62e2bd54510683f9c0bb0d6b'}, NULL, {\"field\": 123}, to_json_boolean(false), [123, 'new_array']]", err: nil},
		},
		{
			name: "not support value test", r: types.MustNewArray(types.Binary{Subtype: types.BinarySubtype(byte(12)), B: []byte("hello")}),
			e: expectedWhereKey{sql: "[", err: fmt.Errorf("The array used in filter contains a datatype not yet supported: types.Binary")},
		},
	}

	for _, field := range prepareArrayForSQLTestCases {
		sqlArray, err := PrepareArrayForSQL(field.r)

		if field.e.err != nil {
			if !strings.EqualFold(sqlArray, field.e.sql) || !strings.Contains(err.Error(), field.e.err.Error()) {
				t.Errorf("%s: whereKey(%v) FAILED. Expected sql = %s and err = %v got sql = %s, sign = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.sign, field.e.err, sqlArray, err)
			}
		} else {
			if !strings.EqualFold(sqlArray, field.e.sql) || err != field.e.err {
				t.Errorf("%s: whereKey(%v) FAILED. Expected sql = %s and err = %v got sql = %s, sign = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.sign, field.e.err, sqlArray, err)
			}
		}
	}
}

type testCaseExpression struct {
	name string
	r1   string
	r2   any
	e    expectedWhereKey
}

func TestLogicExpression(t *testing.T) {
	logicExpressionTestCases := []testCaseExpression{
		{
			name: "AND test", r1: "$and", r2: types.MustNewArray(types.MustMakeDocument("field1", int32(123)), types.MustMakeDocument("field2", "string")),
			e: expectedWhereKey{sql: "(\"field1\" = 123 AND \"field2\" = 'string')", err: nil},
		},
		{
			name: "OR test", r1: "$or", r2: types.MustNewArray(types.MustMakeDocument("field1", int32(123)), types.MustMakeDocument("field2", "string")),
			e: expectedWhereKey{sql: "(\"field1\" = 123 OR \"field2\" = 'string')", err: nil},
		},
		{
			name: "NOR test", r1: "$nor", r2: types.MustNewArray(types.MustMakeDocument("field1", int32(123)), types.MustMakeDocument("field2", "string")),
			e: expectedWhereKey{sql: "( NOT ((\"field1\" = 123 AND \"field1\" IS SET)) AND NOT ((\"field2\" = 'string' AND \"field2\" IS SET)))", err: nil},
		},
		{
			name: "NOR with $elemMatch test", r1: "$nor", r2: types.MustNewArray(types.MustMakeDocument("array_field", types.MustMakeDocument("$elemMatch", types.MustMakeDocument("field", types.MustMakeDocument("new", "doc"))))),
			e: expectedWhereKey{sql: "( NOT (FOR ANY \"element\" IN \"array_field\" SATISFIES \"element\".\"field\" = {\"new\": 'doc'} END ))", err: nil},
		},
		{
			name: "not implemented expression", r1: "$text", r2: "Long text",
			e: expectedWhereKey{sql: "", err: fmt.Errorf("support for $text is not implemented yet")},
		},
		{
			name: "$not as top level error", r1: "$not", r2: types.MustMakeDocument("field", "string"),
			e: expectedWhereKey{sql: "", err: fmt.Errorf("unknown top level: $not. If you are trying to negate an entire expression, use $nor")},
		},
		{
			name: "only one expression in $and error test", r1: "$and", r2: types.MustNewArray(types.MustMakeDocument("field1", int32(123))),
			e: expectedWhereKey{sql: "(", err: fmt.Errorf("Need minimum two expressions")},
		},
		{
			name: "wrong type in array of expression error", r1: "$or", r2: types.MustNewArray("should have been document", "this one too"),
			e: expectedWhereKey{sql: "(", err: fmt.Errorf("Found in array of logicExpression no document but instead the datatype:")},
		},
		{
			name: "logicExpression not used with array error", r1: "$or", r2: "should have been array",
			e: expectedWhereKey{sql: "(", err: fmt.Errorf("Expected an array got string")},
		},
	}

	for _, field := range logicExpressionTestCases {
		sql, err := logicExpression(field.r1, field.r2)

		if field.e.err != nil {
			if !strings.EqualFold(sql, field.e.sql) || !strings.Contains(err.Error(), field.e.err.Error()) {
				t.Errorf("%s: logicExpression(%s, %v) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r1, field.r2, field.e.sql, field.e.err, sql, err)
			}
		} else {
			if !strings.EqualFold(sql, field.e.sql) || err != field.e.err {
				t.Errorf("%s: logicExpression(%s, %v) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r1, field.r2, field.e.sql, field.e.err, sql, err)
			}
		}
	}
}

func TestFieldExpression(t *testing.T) {
	fieldExpressionTestCases := []testCaseExpression{
		{
			name: "greater than test", r1: "field", r2: types.MustMakeDocument("$gt", int32(9)),
			e: expectedWhereKey{sql: "\"field\" > 9", err: nil},
		},
		{
			name: "less than test", r1: "field", r2: types.MustMakeDocument("$lt", int32(9)),
			e: expectedWhereKey{sql: "\"field\" < 9", err: nil},
		},
		{
			name: "greater than or equal test", r1: "field", r2: types.MustMakeDocument("$gte", int32(9)),
			e: expectedWhereKey{sql: "\"field\" >= 9", err: nil},
		},
		{
			name: "less than or equal test", r1: "field", r2: types.MustMakeDocument("$lte", int32(9)),
			e: expectedWhereKey{sql: "\"field\" <= 9", err: nil},
		},
		{
			name: "equal test", r1: "field", r2: types.MustMakeDocument("$eq", int32(9)),
			e: expectedWhereKey{sql: "\"field\" = 9", err: nil},
		},
		{
			name: "not equal test", r1: "field", r2: types.MustMakeDocument("$ne", int32(9)),
			e: expectedWhereKey{sql: "(\"field\" <> 9 OR \"field\" IS UNSET)", err: nil},
		},
		{
			name: "exists test", r1: "field", r2: types.MustMakeDocument("$exists", true),
			e: expectedWhereKey{sql: "\"field\" IS SET", err: nil},
		},
		{
			name: "array size test", r1: "field", r2: types.MustMakeDocument("$size", int32(9)),
			e: expectedWhereKey{sql: "CARDINALITY(\"field\") = 9", err: nil},
		},
		{
			name: "$all test", r1: "field", r2: types.MustMakeDocument("$all", types.MustNewArray(int32(9), "string")),
			e: expectedWhereKey{sql: "FOR ANY \"element\" IN \"field\" SATISFIES \"element\" = 9 END  AND FOR ANY \"element\" IN \"field\" SATISFIES \"element\" = 'string' END ", err: nil},
		},
		{
			name: "$elemMatch test", r1: "field", r2: types.MustMakeDocument("$elemMatch", types.MustMakeDocument("$gt", int32(9))),
			e: expectedWhereKey{sql: "FOR ANY \"element\" IN \"field\" SATISFIES \"element\" > 9 END ", err: nil},
		},
		{
			name: "not test", r1: "field", r2: types.MustMakeDocument("$not", types.MustMakeDocument("$gt", int32(9))),
			e: expectedWhereKey{sql: "( NOT \"field\" > 9 OR \"field\" IS UNSET) ", err: nil},
		},
		{
			name: "$regex test", r1: "field", r2: types.MustMakeDocument("$regex", "pattern"),
			e: expectedWhereKey{sql: "\"field\" LIKE '%pattern%'", err: nil},
		},
		{
			name: "fieldExpression not used with document error test", r1: "field", r2: "should have been a document",
			e: expectedWhereKey{sql: "", err: fmt.Errorf("In use of field expression a document was expected. Got instead: string")},
		},
		{
			name: "$exists: false test", r1: "field", r2: types.MustMakeDocument("$exists", false),
			e: expectedWhereKey{sql: "\"field\" IS UNSET", err: nil},
		},
		{
			name: "$exists not used with boolean error test", r1: "field", r2: types.MustMakeDocument("$exists", int32(1)),
			e: expectedWhereKey{sql: "", err: fmt.Errorf("$exists only works with true or false")},
		},
		{
			name: "not supported expression error test", r1: "field", r2: types.MustMakeDocument("$geoWithin", "not supported"),
			e: expectedWhereKey{sql: "\"field\"", err: fmt.Errorf("support for $geoWithin is not implemented yet")},
		},
	}

	for _, field := range fieldExpressionTestCases {
		sql, err := fieldExpression(field.r1, field.r2)

		if field.e.err != nil {
			if !strings.EqualFold(sql, field.e.sql) || !strings.Contains(err.Error(), field.e.err.Error()) {
				t.Errorf("%s: fieldExpression(%s, %v) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r1, field.r2, field.e.sql, field.e.err, sql, err)
			}
		} else {
			if !strings.EqualFold(sql, field.e.sql) || err != field.e.err {
				t.Errorf("%s: fieldExpression(%s, %v) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r1, field.r2, field.e.sql, field.e.err, sql, err)
			}
		}
	}
}

type testCaseFilterArray struct {
	name string
	r1   string
	r2   string
	r3   any
	e    expectedWhereKey
}

func TestFilterArray(t *testing.T) {
	filterArrayTestCases := []testCaseFilterArray{
		{
			name: "$elemMatch with comparison test", r1: "\"nested\".\"field\"", r2: "elemMatch", r3: types.MustMakeDocument("$gte", int32(9)),
			e: expectedWhereKey{sql: "FOR ANY \"element\" IN \"nested\".\"field\" SATISFIES \"element\" >= 9 END ", err: nil},
		},
		{
			name: "$elemMatch with field: value test", r1: "\"nested\".\"field\"", r2: "elemMatch", r3: types.MustMakeDocument("field", float64(14.241234)),
			e: expectedWhereKey{sql: "FOR ANY \"element\" IN \"nested\".\"field\" SATISFIES \"element\".\"field\" = 14.241234 END ", err: nil},
		},
		{
			name: "$all test", r1: "\"nested\".\"field\"", r2: "all", r3: types.MustNewArray("field", float64(14.241234)),
			e: expectedWhereKey{sql: "FOR ANY \"element\" IN \"nested\".\"field\" SATISFIES \"element\" = 'field' END  AND FOR ANY \"element\" IN \"nested\".\"field\" SATISFIES \"element\" = 14.241234 END ", err: nil},
		},
		{
			name: "not using array with $all error test", r1: "field", r2: "all", r3: "should have been array",
			e: expectedWhereKey{sql: "", err: fmt.Errorf("If $all: Expected array. If $elemMatch: Expected document. Got instead: string")},
		},
		{
			name: "$all used with document error test", r1: "\"nested\".\"field\"", r2: "all", r3: types.MustMakeDocument("field", float64(14.241234)),
			e: expectedWhereKey{sql: "", err: fmt.Errorf("$all requires an array of expression not a document")},
		},
		{
			name: "$elemMatch used with array error test", r1: "\"nested\".\"field\"", r2: "elemMatch", r3: types.MustNewArray("$gte", int32(9), "$lte", int64(11)),
			e: expectedWhereKey{sql: "", err: fmt.Errorf("$elemMatch requires a document of expression not an array")},
		},
	}

	for _, field := range filterArrayTestCases {
		sql, err := filterArray(field.r1, field.r2, field.r3)

		if field.e.err != nil {
			if !strings.EqualFold(sql, field.e.sql) || !strings.Contains(err.Error(), field.e.err.Error()) {
				t.Errorf("%s: filterArray(%s, %s, %v) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r1, field.r2, field.r3, field.e.sql, field.e.err, sql, err)
			}
		} else {
			if !strings.EqualFold(sql, field.e.sql) || err != field.e.err {
				t.Errorf("%s: filterArray(%s, %s, %v) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r1, field.r2, field.r3, field.e.sql, field.e.err, sql, err)
			}
		}
	}
}

func TestRegex(t *testing.T) {
	regexTestCases := []testCaseWhereValue{
		{name: "test regex", r: "pattern", e: expectedWhereKey{sql: "'%pattern%'", err: nil}},
		{name: "wrong value for $regex", r: int32(2), e: expectedWhereKey{sql: "", err: fmt.Errorf("Expected either a JavaScript regular expression objects (i.e. /pattern/) or string containing a pattern. Got instead type int32")}},
	}

	for _, field := range regexTestCases {
		sql, err := regex(field.r)

		if field.e.err != nil {
			if !strings.EqualFold(sql, field.e.sql) || !strings.Contains(err.Error(), field.e.err.Error()) {
				t.Errorf("%s: where(%v) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.err, sql, err)
			}
		} else {
			if !strings.EqualFold(sql, field.e.sql) || err != field.e.err {
				t.Errorf("%s: where(%v) FAILED. Expected sql = %s and err = %v got sql = %s and err = %v", field.name,
					field.r, field.e.sql, field.e.err, sql, err)
			}
		}
	}
}
