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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArray(t *testing.T) {
	t.Parallel()

	t.Run("ZeroValues", func(t *testing.T) {
		t.Parallel()

		// to avoid []any != nil in tests
		assert.Nil(t, MustNewArray().s)
		assert.Nil(t, MakeArray(0).s)

		var a Array
		assert.Equal(t, 0, a.Len())
		assert.Nil(t, a.s)

		err := a.Append(nil)
		assert.NoError(t, err)
		value, err := a.Get(0)
		assert.NoError(t, err)
		assert.Equal(t, nil, value)

		err = a.Append(42)
		assert.EqualError(t, err, `types.Array.Append: types.validateValue: unsupported type: int (42)`)
	})

	t.Run("NewArray with unsupported type", func(t *testing.T) {
		t.Parallel()

		a, err := NewArray(int32(42), 42)
		assert.Nil(t, a)
		assert.EqualError(t, err, `types.NewArray: index 1: types.validateValue: unsupported type: int (42)`)
	})

	t.Run("Subslice", func(t *testing.T) {
		t.Parallel()

		a, _ := NewArray(int32(1), int32(2), int32(3), int32(4), int32(5))
		aSub, _ := a.Subslice(1, 3)
		assert.Equal(t, MustNewArray(int32(2), int32(3)), aSub)

		errSub, err := aSub.Subslice(1, 5)
		assert.Nil(t, errSub)
		assert.EqualError(t, err, `types.Array.Subslice: high index 5 is out of bounds [0-2)`)

		errSub, err = aSub.Subslice(3, 5)
		assert.Nil(t, errSub)
		assert.EqualError(t, err, `types.Array.Subslice: low index 3 is out of bounds [0-2)`)
		errSub, err = aSub.Subslice(1, 0)
		assert.Nil(t, errSub)
		assert.EqualError(t, err, `types.Array.Subslice: high index 0 is less low index 1`)
	})

	t.Run("Set", func(t *testing.T) {
		t.Parallel()

		a, _ := NewArray(int32(1), int32(1), int32(1))

		a.Set(0, "index 0")
		a.Set(2, "index 2")
		a.Set(1, "index 1")

		assert.Equal(t, MustNewArray("index 0", "index 1", "index 2"), a)

		err := a.Set(3, "index 3")
		assert.EqualError(t, err, `types.Array.Set: index 3 is out of bounds [0-3)`)

		err = a.Set(2, 2)
		assert.EqualError(t, err, `types.Array.Set: types.validateValue: unsupported type: int (2)`)
	})

	t.Run("Contains", func(t *testing.T) {
		t.Parallel()

		a, _ := NewArray("key1", "key2", "key3")

		b := a.Contains("key2")

		assert.True(t, b)

		b = a.Contains("keY2")

		assert.False(t, b)
	})

	t.Run("Append and Delete", func(t *testing.T) {
		t.Parallel()

		a, _ := NewArray()

		a.Append(int32(1), true, false)

		assert.Equal(t, MustNewArray(int32(1), true, false), a)

		a.Delete(0)
		assert.Equal(t, MustNewArray(true, false), a)

		err := a.Delete(2)
		assert.Equal(t, MustNewArray(true, false), a)
		assert.EqualError(t, err, `types.Array.Delete: index 2 is out of bounds [0-2)`)
	})

	t.Run("Append and Delete", func(t *testing.T) {
		t.Parallel()

		a, _ := NewArray("key1", "key2", "key3")

		ptr, err := a.GetPointer(3)
		assert.Nil(t, ptr)
		assert.EqualError(t, err, `types.Array.Get: index 3 is out of bounds [0-3)`)

		ptr, err = a.GetPointer(2)
		assert.Nil(t, err)
		assert.EqualValues(t, any("key3"), *ptr)
	})
}
