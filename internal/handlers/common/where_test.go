package common

import (
	"fmt"
	"strings"
	"testing"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
)

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
		{name: "document test", r: types.MustMakeDocument(
			"bool", true,
			"int32", int32(0),
			"int64", int64(223372036854775807),
			"objectID", types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107},
			"string", "foo",
			"null", nil),
			e: expectedWhereKey{sql: "{\"bool\": to_json_boolean(true), \"int32\": 0, \"int64\": 223372036854775807, \"objectID\": {\"oid\":'62e2bd54510683f9c0bb0d6b'}, \"string\": 'foo', \"null\":  NULL }", sign: " = ", err: nil}},
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
