// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"testing"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestUpdate(t *testing.T) {
	t.Run("set fields with supported and unsupported values", func(t *testing.T) {
		t.Parallel()

		updateSQL, notWhereSQL, err := update(types.MustMakeDocument("$set", types.MustMakeDocument("str_value", "value", "int32_value", int32(123), "int64_value", int64(223372036854775807), "float64_value", 64534.12432, "bool_value", true, "objID_value", types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107}, "document_value", types.MustMakeDocument("string", "value", "int32", int32(2), "int64", int64(4543654563), "float", float64(543245.2245), "bool", true, "array", types.MustNewArray(int32(1), "2"), "nested_docu", types.MustMakeDocument("inside", "array"), "objID", types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107}, "null", nil), "null_value", nil, "nested.field", "value", "nested.field.array.2", int32(12))))

		assert.Equal(t, " SET \"str_value\" = 'value', \"int32_value\" = 123, \"int64_value\" = 223372036854775807, \"float64_value\" = 64534.124320, \"bool_value\" = to_json_boolean(true), \"objID_value\" = {\"oid\":'62e2bd54510683f9c0bb0d6b'}, \"document_value\" = {\"string\": 'value', \"int32\": 2, \"int64\": 4543654563, \"float\": 543245.224500, \"bool\": to_json_boolean(true), \"array\": [1, '2'], \"nested_docu\": {\"inside\": 'array'}, \"objID\": {\"oid\":'62e2bd54510683f9c0bb0d6b'}, \"null\":  NULL }, \"null_value\" = NULL, \"nested\".\"field\" = 'value', \"nested\".\"field\".\"array\"[3] = 12", updateSQL)
		assert.Equal(t, " AND ( NOT (   \"str_value\" = 'value' AND \"int32_value\" = 123 AND \"int64_value\" = 223372036854775807 AND \"float64_value\" = 64534.124320 AND \"bool_value\" = to_json_boolean(true) AND \"objID_value\" = {\"oid\":'62e2bd54510683f9c0bb0d6b'} AND \"document_value\" = {\"string\": 'value', \"int32\": 2, \"int64\": 4543654563, \"float\": 543245.224500, \"bool\": to_json_boolean(true), \"array\": [1, '2'], \"nested_docu\": {\"inside\": 'array'}, \"objID\": {\"oid\":'62e2bd54510683f9c0bb0d6b'}, \"null\":  NULL } AND \"null_value\" IS NULL AND \"nested\".\"field\" = 'value' AND \"nested\".\"field\".\"array\"[3] = 12) OR (\"str_value\" IS UNSET OR \"int32_value\" IS UNSET OR \"int64_value\" IS UNSET OR \"float64_value\" IS UNSET OR \"bool_value\" IS UNSET OR \"objID_value\" IS UNSET OR \"document_value\" IS UNSET OR \"null_value\" IS UNSET OR \"nested\".\"field\" IS UNSET OR \"nested\".\"field\".\"array\"[3] IS UNSET )) ", notWhereSQL)
		assert.Nil(t, err)

		updateSQL, notWhereSQL, err = update(types.MustMakeDocument("$set", types.MustMakeDocument("array", types.MustNewArray(int32(1), "2"))))

		assert.Equal(t, " SET \"array\" = [1, '2']", updateSQL)
		assert.Equal(t, " WHERE ", notWhereSQL)
		assert.EqualError(t, err, `<msg_update.go:232 crud.update> Cannot update field with array`)

		updateSQL, notWhereSQL, err = update(types.MustMakeDocument("$set", types.MustMakeDocument("_id", types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107})))

		assert.Equal(t, " SET ", updateSQL)
		assert.Equal(t, "", notWhereSQL)
		assert.EqualError(t, err, `performing an update on the path '_id' would modify the immutable field '_id'`)

		updateSQL, notWhereSQL, err = update(types.MustMakeDocument("$set", types.MustMakeDocument("array.2.3", types.ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107})))

		assert.Equal(t, " SET ", updateSQL)
		assert.Equal(t, "", notWhereSQL)
		assert.EqualError(t, err, `<msg_update.go:329 crud.getUpdateKey> Not allowed to index on an array inside of an array.`)

		updateSQL, notWhereSQL, err = update(types.MustMakeDocument("$set", types.MustMakeDocument("unsupported value", types.Binary{Subtype: types.BinarySubtype(byte(12)), B: []byte("hello")})))

		assert.Equal(t, " SET ", updateSQL)
		assert.Equal(t, "", notWhereSQL)
		assert.EqualError(t, err, `<msg_update.go:400 crud.getUpdateValue> Value: types.Binary is not supported for update`)
	})

	t.Run("unset fields with supported and unsupported values", func(t *testing.T) {
		t.Parallel()

		updateSQL, notWhereSQL, err := update(types.MustMakeDocument("$unset", types.MustMakeDocument("field1", "", "field2", int32(123))))

		assert.Equal(t, " UNSET \"field1\", \"field2\"", updateSQL)
		assert.Equal(t, " AND ( \"field1\" IS SET OR \"field2\" IS SET )", notWhereSQL)
		assert.Nil(t, err)

		updateSQL, notWhereSQL, err = update(types.MustMakeDocument("$unset", types.MustMakeDocument("_id", "")))

		assert.Equal(t, "", updateSQL)
		assert.Equal(t, "", notWhereSQL)
		assert.EqualError(t, err, `performing an update on the path '_id' would modify the immutable field '_id'`)
	})

	t.Run("unset and unset fields with supported and unsupported values", func(t *testing.T) {
		t.Parallel()

		updateSQL, notWhereSQL, err := update(types.MustMakeDocument("$unset", types.MustMakeDocument("field1", "", "field2", int32(123)), "$set", types.MustMakeDocument("field3", int32(123))))

		assert.Equal(t, " SET \"field3\" = 123,  UNSET \"field1\", \"field2\"", updateSQL)
		assert.Equal(t, " AND ( NOT (   \"field3\" = 123) OR (\"field3\" IS UNSET ) OR ( \"field1\" IS SET OR \"field2\" IS SET ))", notWhereSQL)
		assert.Nil(t, err)

		updateSQL, notWhereSQL, err = update(types.MustMakeDocument("$unset", types.MustMakeDocument("_id", ""), "$set", types.MustMakeDocument("field", "value")))

		assert.Equal(t, " SET \"field\" = 'value'", updateSQL)
		assert.Equal(t, "", notWhereSQL)
		assert.EqualError(t, err, `performing an update on the path '_id' would modify the immutable field '_id'`)

		updateSQL, notWhereSQL, err = update(types.MustMakeDocument("$unset", types.MustMakeDocument("field1", ""), "$set", types.MustMakeDocument("array", types.MustNewArray(int32(1), "2"))))

		assert.Equal(t, " SET \"array\" = [1, '2']", updateSQL)
		assert.Equal(t, " WHERE ", notWhereSQL)
		assert.EqualError(t, err, `<msg_update.go:220 crud.update> Cannot update field with array`)
	})
}
