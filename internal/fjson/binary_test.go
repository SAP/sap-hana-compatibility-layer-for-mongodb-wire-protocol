// SPDX-FileCopyrightText: 2021 FerretDB Inc.
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

// import (
// 	"testing"

// 	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
// )

// var binaryTestCases = []testCase{{
// 	name: "foo",
// 	v: &Binary{
// 		Subtype: types.BinaryUser,
// 		B:       []byte("foo"),
// 	},
// 	j: `{"bin":"Zm9v","s":128}`,
// }, {
// 	name: "empty",
// 	v: &Binary{
// 		Subtype: types.BinaryGeneric,
// 		B:       []byte{},
// 	},
// 	j:      `{"bin":""}`,
// 	canonJ: `{"bin":"","s":0}`,
// }, {
// 	name: "invalid subtype",
// 	v: &Binary{
// 		Subtype: 0xff,
// 		B:       []byte{},
// 	},
// 	j: `{"bin":"","s":255}`,
// }, {
// 	name: "extra JSON fields",
// 	v: &Binary{
// 		Subtype: types.BinaryUser,
// 		B:       []byte("foo"),
// 	},
// 	j:      `{"bin":"Zm9v","s":128,"foo":"bar"}`,
// 	canonJ: `{"bin":"Zm9v","s":128}`,
// 	jErr:   `json: unknown field "foo"`,
// }, {
// 	name: "EOF",
// 	j:    `{`,
// 	jErr: `unexpected EOF`,
// }}

// func TestBinary(t *testing.T) {
// 	t.Parallel()
// 	testJSON(t, binaryTestCases, func() fjsontype { return new(Binary) })
// }

// func FuzzBinary(f *testing.F) {
// 	fuzzJSON(f, binaryTestCases, func() fjsontype { return new(Binary) })
// }

// func BenchmarkBinary(b *testing.B) {
// 	benchmark(b, binaryTestCases, func() fjsontype { return new(Binary) })
// }
