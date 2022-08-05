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

package fjson

import (
	"testing"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
)

func convertArray(a *types.Array) *Array {
	res := Array(*a)
	return &res
}

var arrayTestCases = []testCase{{
	name: "array_all",
	v: convertArray(types.MustNewArray(
		types.MustNewArray(),
		true,
		types.MustMakeDocument(),
		42.13,
		int32(42),
		int64(223372036854775807),
		"foo",
		nil,
	)),
	j: `[[],true,{},42.13,42,223372036854775807,"foo",null]`,
}, {
	name: "EOF",
	j:    `[`,
	jErr: `unexpected EOF`,
}}

func TestArray(t *testing.T) {
	t.Parallel()
	testJSON(t, arrayTestCases, func() fjsontype { return new(Array) })
}

func FuzzArray(f *testing.F) {
	fuzzJSON(f, arrayTestCases, func() fjsontype { return new(Array) })
}

func BenchmarkArray(b *testing.B) {
	benchmark(b, arrayTestCases, func() fjsontype { return new(Array) })
}
