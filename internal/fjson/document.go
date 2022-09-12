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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
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

	jsonKeys, err := getJSONKeys(data)
	if err != nil {
		return lazyerrors.Error(err)
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

	td := types.MustMakeDocument()

	for _, key := range jsonKeys {
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

// getJSONKeys returns a slice containing the fields of the JSON document. This enables order preservance.
func getJSONKeys(docs []byte) (keys []string, error error) {
	r := bytes.NewReader(docs)
	dec := json.NewDecoder(r)
	i := 0
	isArray := 0
	isDocument := 0
	for {

		t, err := dec.Token()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		if i == 0 {
			i++
			continue
		}

		s := fmt.Sprintf("%v", t)

		if s == "]" {
			isArray--
			continue
		}

		if s == "}" {
			isDocument--
			continue
		}

		if s == "[" {
			i++
			isArray++
			continue
		}

		if s == "{" {
			i++
			isDocument++
			continue
		}

		// Continues when value
		if i%2 == 0 {
			i++
			continue
		}

		if isArray > 0 || isDocument > 0 {
			continue
		}

		keys = append(keys, fmt.Sprintf("%v", t))

		i++
	}

	return keys, nil
}

// MarshalJSON implements fjsontype interface. This is used by the wire protocol
func (doc *Document) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	var b []byte
	var err error
	var idInserted bool

	td := types.Document(*doc)

	buf.WriteByte('{')

	objectId, _ := td.Get("_id")
	// Puts field _id in the front of the document
	switch objectId := objectId.(type) {
	case types.ObjectID:
		buf.Write([]byte("\"_id\""))
		buf.WriteByte(':')
		b, err = Marshal(objectId)

		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)

		idInserted = true
	default:
		idInserted = false
	}

	i := 0

	for _, key := range td.Keys() {

		if key == "_id" && idInserted {
			continue
		}

		if i != 0 || idInserted {
			buf.WriteByte(',')
		}

		if b, err = json.Marshal(key); err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)
		buf.WriteByte(':')

		value, err := td.Get(key)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		b, err = Marshal(value)

		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)
		i++
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}

// MarshalJSONHANA implements fjsontype interface. This is used by MongoDB operations.
func (doc *Document) MarshalJSONHANA() ([]byte, error) {
	var buf bytes.Buffer
	var b []byte
	var err error
	var idInserted bool

	td := types.Document(*doc)

	buf.WriteByte('{')

	objectId, _ := td.Get("_id")
	// Puts field _id in the front of the document
	switch objectId := objectId.(type) {
	case types.ObjectID:
		buf.Write([]byte("\"_id\""))
		buf.WriteByte(':')
		b, err = MarshalHANA(objectId)

		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)

		idInserted = true
	default:
		idInserted = false
	}

	i := 0

	for _, key := range td.Keys() {

		if key == "_id" && idInserted {
			continue
		}

		if i != 0 || idInserted {
			buf.WriteByte(',')
		}

		if b, err = json.Marshal(key); err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)
		buf.WriteByte(':')

		value, err := td.Get(key)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		b, err = MarshalHANA(value)

		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)
		i++
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}

// check interfaces
var (
	_ fjsontype = (*Document)(nil)
)
