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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		doc  Document
		err  error
	}{{
		name: "normal",
		doc: Document{
			keys: []string{"0"},
			m:    map[string]any{"0": "foo"},
		},
	}, {
		name: "empty",
		doc:  Document{},
	}, {
		name: "different keys",
		doc: Document{
			keys: []string{"0"},
			m:    map[string]any{"1": "foo"},
		},
		err: fmt.Errorf(`types.Document.validate: key not found: "0"`),
	}, {
		name: "duplicate keys",
		doc: Document{
			keys: []string{"0", "0"},
			m:    map[string]any{"0": "foo"},
		},
		err: fmt.Errorf("types.Document.validate: keys and values count mismatch: 1 != 2"),
	}, {
		name: "duplicate and different keys",
		doc: Document{
			keys: []string{"0", "0"},
			m:    map[string]any{"0": "foo", "1": "bar"},
		},
		err: fmt.Errorf(`types.Document.validate: duplicate key: "0"`),
	}, {
		name: "fjson keys",
		doc: Document{
			keys: []string{"$k"},
			m:    map[string]any{"$k": "foo"},
		},
		err: fmt.Errorf(`types.Document.validate: invalid key: "$k"`),
	}} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.doc.validate()
			assert.Equal(t, tc.err, err)
		})
	}

	t.Run("convertDocument", func(t *testing.T) {
		t.Parallel()

		d := MustMakeDocument("field", "value")

		convertedDoc := MustConvertDocument(d)

		assert.Equal(t, MustMakeDocument("field", "value"), convertedDoc)

		m := map[string]any{"field": 12}

		var k []string
		k = append(k, "field")

		d1 := Document{m: m, keys: k}

		convertedDoc, err := ConvertDocument(d1)
		assert.Equal(t, d1, convertedDoc)
		assert.EqualError(t, err, `types.ConvertDocument: types.Document.validate: types.validateValue: unsupported type: int (12)`)

		k = nil
		m = nil
		d2 := Document{m: m, keys: k}
		convertedDoc, err = ConvertDocument(d2)
		expectDoc := Document{map[string]any{}, []string{}}
		assert.Nil(t, err)
		assert.Equal(t, expectDoc, convertedDoc)
	})

	t.Run("Set and Remove", func(t *testing.T) {
		t.Parallel()

		d := MustMakeDocument("field1", "value", "field2", int32(1))

		d.Remove("field1")

		assert.Equal(t, MustMakeDocument("field2", int32(1)), d)

		d.Remove("field1")

		assert.Equal(t, MustMakeDocument("field2", int32(1)), d)

		err := d.Set("field1", "value")
		assert.Nil(t, err)
		assert.Equal(t, MustMakeDocument("field2", int32(1), "field1", "value"), d)

		err = d.Set("field3", 12)
		assert.EqualError(t, err, `types.Document.validate: types.validateValue: unsupported type: int (12)`)

		err = d.Set("", int32(12))
		assert.EqualError(t, err, `types.Document.Set: invalid key: ""`)

	})
}
