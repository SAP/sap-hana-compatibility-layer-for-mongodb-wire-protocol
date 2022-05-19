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
	"bytes"
	"encoding/json"

	"github.com/lucboj/FerretDB_SAP_HANA/internal/types"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/util/lazyerrors"
)

// Document represents BSON Document data type.
type Document types.Document

// fjsontype implements fjsontype interface.
func (doc *Document) fjsontype() {}

// UnmarshalJSON implements fjsontype interface.
func (doc *Document) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		panic("null data")
	}

	r := bytes.NewReader(data)
	dec := json.NewDecoder(r)

	var rawMessages map[string]json.RawMessage
	if err := dec.Decode(&rawMessages); err != nil {
		return lazyerrors.Error(err)
	}
	if err := checkConsumed(dec, r); err != nil {
		return lazyerrors.Error(err)
	}

	bKeys, ok := rawMessages["keys"]
	if !ok {
		return lazyerrors.Errorf("fjson.Document.Unmarshal: missing keys")
	}

	var keys []string

	if err := json.Unmarshal(bKeys, &keys); err != nil {
		if err.Error() == "json: cannot unmarshal string into Go value of type []string" {

			var newbKeys []byte
			for _, k := range bKeys {
				if bytes.Equal([]byte{k}, []byte{92}) {
					continue
				}
				newbKeys = append(newbKeys, k)
			}
			if err := json.Unmarshal(newbKeys[1:(len(newbKeys)-1)], &keys); err != nil {
				return lazyerrors.Error(err)
			}
		} else {
			return lazyerrors.Error(err)
		}

	}

	td := types.MustMakeDocument()

	for _, key := range keys {
		bValue, ok := rawMessages[key]
		if !ok {
			return lazyerrors.Errorf("fjson.Document.UnmarshalJSON: missing key %q", key)
		}

		value, err := Unmarshal(bValue)
		if err != nil {
			return lazyerrors.Error(err)
		}
		if err = td.Set(key, value); err != nil {
			return lazyerrors.Error(err)
		}

	}

	*doc = Document(td)
	return nil
}

// MarshalJSON implements fjsontype interface.
func (doc *Document) MarshalJSON() ([]byte, error) {

	td := types.Document(*doc)
	var buf bytes.Buffer

	buf.WriteString(`{"$k":`)
	b, err := json.Marshal(td.Keys())
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	buf.Write(b)

	for _, key := range td.Keys() {
		buf.WriteByte(',')

		if b, err = json.Marshal(key); err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)
		buf.WriteByte(':')

		value, err := td.Get(key)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
		b, err := Marshal(value)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)
	}

	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func MarshalJSONHANA(doc types.Document) ([]byte, error) {

	var buf bytes.Buffer
	var b []byte
	var err error

	buf.WriteString("{\"keys\":")
	b, err = json.Marshal(doc.Keys())
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	buf.Write(b)

	for _, key := range doc.Keys() {

		buf.WriteByte(',')

		if b, err = json.Marshal(key); err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)
		buf.WriteByte(':')

		value, err := doc.Get(key)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		switch value := value.(type) {
		case types.Document:
			b, err = MarshalHANA(value)
		default:
			b, err = Marshal(value)
		}

		c := []byte{123, 34, 36, 111, 34}
		res := bytes.Contains(b, c)
		if res {
			cAdd := []byte{123, 34, 111, 105, 100, 34}
			b = append(cAdd, b[5:]...)
		}

		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)
	}

	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// check interfaces
var (
	_ fjsontype = (*Document)(nil)
)
