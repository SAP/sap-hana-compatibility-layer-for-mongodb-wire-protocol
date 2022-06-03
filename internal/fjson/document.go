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

	"github.com/DocStore/HANA_HWY/internal/types"
	"github.com/DocStore/HANA_HWY/internal/util/lazyerrors"
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

		if i%2 == 0 {
			i++
			continue
		}

		if s == "]" {
			isArray--
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

	buf.WriteByte('{')

	i := 0

	for _, key := range doc.Keys() {

		if i != 0 {
			buf.WriteByte(',')
		}

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
