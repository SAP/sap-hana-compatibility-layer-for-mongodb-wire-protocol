// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	t.Run("Compare float64 with _", func(t *testing.T) {
		t.Parallel()

		fl := float64(123.234)

		result := CompareScalars(fl, float64(123.234))
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(fl, int32(1))
		assert.Equal(t, CompareResult(2), result)

		result = CompareScalars(fl, int64(123123123123))
		assert.Equal(t, CompareResult(1), result)

		result = CompareScalars(fl, true)
		assert.Equal(t, CompareResult(3), result)
	})

	t.Run("Compare string with _", func(t *testing.T) {
		t.Parallel()

		str := "string"

		result := CompareScalars(str, "string")
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(str, true)
		assert.Equal(t, CompareResult(3), result)
	})

	t.Run("Compare objectID with _", func(t *testing.T) {
		t.Parallel()

		objID := ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107}

		result := CompareScalars(objID, ObjectID{98, 226, 189, 84, 81, 6, 131, 249, 192, 187, 13, 107})
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(objID, ObjectID{99, 227, 190, 85, 82, 6, 131, 249, 192, 187, 13, 107})
		assert.Equal(t, CompareResult(1), result)

		result = CompareScalars(objID, ObjectID{97, 225, 188, 83, 81, 6, 131, 249, 192, 187, 13, 107})
		assert.Equal(t, CompareResult(2), result)

		result = CompareScalars(objID, "not objID")
		assert.Equal(t, CompareResult(3), result)
	})

	t.Run("Compare bool with _", func(t *testing.T) {
		t.Parallel()

		boolVal := true

		result := CompareScalars(boolVal, true)
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(boolVal, false)
		assert.Equal(t, CompareResult(2), result)

		result = CompareScalars(false, boolVal)
		assert.Equal(t, CompareResult(1), result)

		result = CompareScalars(boolVal, "not bool")
		assert.Equal(t, CompareResult(3), result)
	})

	t.Run("Compare Time.time with _", func(t *testing.T) {
		t.Parallel()

		compTime := time.Date(2021, time.Month(2), 21, 1, 10, 30, 0, time.UTC)

		result := CompareScalars(compTime, time.Date(2021, time.Month(2), 21, 1, 10, 30, 0, time.UTC))
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(compTime, time.Date(2020, time.Month(3), 22, 1, 10, 30, 0, time.UTC))
		assert.Equal(t, CompareResult(2), result)

		result = CompareScalars(time.Date(2020, time.Month(3), 22, 1, 10, 30, 0, time.UTC), compTime)
		assert.Equal(t, CompareResult(1), result)

		result = CompareScalars(compTime, "not time")
		assert.Equal(t, CompareResult(3), result)
	})

	t.Run("Compare regex with _", func(t *testing.T) {
		t.Parallel()

		regex := Regex{Pattern: "pattern"}

		result := CompareScalars(regex, "not regex")
		assert.Equal(t, CompareResult(3), result)
	})

	t.Run("Compare int32 with _", func(t *testing.T) {
		t.Parallel()

		integer32 := int32(3)

		result := CompareScalars(integer32, int32(3))
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(integer32, int32(2))
		assert.Equal(t, CompareResult(2), result)

		result = CompareScalars(integer32, int32(4))
		assert.Equal(t, CompareResult(1), result)

		result = CompareScalars(integer32, int32(3))
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(integer32, float64(3.5))
		assert.Equal(t, CompareResult(1), result)

		result = CompareScalars(integer32, float64(3.000))
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(integer32, int64(3))
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(integer32, false)
		assert.Equal(t, CompareResult(3), result)
	})

	t.Run("Compare Timestamp with _", func(t *testing.T) {
		t.Parallel()

		stamp := Timestamp(123)

		result := CompareScalars(stamp, Timestamp(123))
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(stamp, Timestamp(124))
		assert.Equal(t, CompareResult(1), result)

		result = CompareScalars(stamp, Timestamp(122))
		assert.Equal(t, CompareResult(2), result)

		result = CompareScalars(stamp, false)
		assert.Equal(t, CompareResult(3), result)
	})

	t.Run("Compare int64 with _", func(t *testing.T) {
		t.Parallel()

		integer64 := int64(223372036854775807)
		result := CompareScalars(integer64, int64(223372036854775807))
		assert.Equal(t, CompareResult(0), result)

		result = CompareScalars(integer64, int64(223372036854))
		assert.Equal(t, CompareResult(2), result)

		result = CompareScalars(integer64, int64(323372036854775807))
		assert.Equal(t, CompareResult(1), result)

		result = CompareScalars(integer64, float64(1.2))
		assert.Equal(t, CompareResult(2), result)

		result = CompareScalars(integer64, int32(1))
		assert.Equal(t, CompareResult(2), result)

		result = CompareScalars(integer64, "not int")
		assert.Equal(t, CompareResult(3), result)

	})
}
