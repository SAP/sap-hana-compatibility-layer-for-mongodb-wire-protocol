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

	fmt.Println("RawMessage:")
	fmt.Println(rawMessages)
	//-----WOrks for collection TEST----
	bKeys, ok := rawMessages["keys"]
	if !ok {
		return lazyerrors.Errorf("fjson.Document.Unmarshal: missing keys")
	}
	fmt.Println("Im here1")
	var keys []string
	fmt.Println("Im here2")
	fmt.Println(bKeys)
	if err := json.Unmarshal(bKeys, &keys); err != nil {
		if err.Error() == "json: cannot unmarshal string into Go value of type []string" {
			fmt.Println("was here")
			fmt.Println(bKeys[1:(len(bKeys) - 1)])
			var newbKeys []byte
			for _, k := range bKeys {
				if bytes.Equal([]byte{k}, []byte{92}) {
					continue
				}
				newbKeys = append(newbKeys, k)
				//fmt.Println(k)
			}
			fmt.Println(newbKeys)
			if err := json.Unmarshal(newbKeys[1:(len(newbKeys)-1)], &keys); err != nil {
				return lazyerrors.Error(err)
			}
		} else {
			return lazyerrors.Error(err)
		}

	}
	fmt.Println("Im here3")
	td := types.MustMakeDocument()
	fmt.Println("hey")
	for _, key := range keys {
		bValue, ok := rawMessages[key]
		if !ok {
			return lazyerrors.Errorf("fjson.Document.UnmarshalJSON: missing key %q", key)
		}
		fmt.Println(key)
		value, err := Unmarshal(bValue)
		if err != nil {
			return lazyerrors.Error(err)
		}
		if err = td.Set(key, value); err != nil {
			return lazyerrors.Error(err)
		}

	}
	//-----WOrks for collection TEST----

	//b, ok := rawMessages["$k"]
	//if !ok {
	//	return lazyerrors.Errorf("fjson.Document.UnmarshalJSON: missing $k")
	//}

	//var keys []string
	//if err := json.Unmarshal(b, &keys); err != nil {
	//	return lazyerrors.Error(err)
	//}
	//if len(keys)+1 != len(rawMessages) {
	//	return lazyerrors.Errorf("fjson.Document.UnmarshalJSON: %d elements in $k, %d in total", len(keys), len(rawMessages))
	//}

	//td := types.MustMakeDocument()
	//for _, key := range keys {
	//	b, ok = rawMessages[key]
	//	if !ok {
	//		return lazyerrors.Errorf("fjson.Document.UnmarshalJSON: missing key %q", key)
	//	}
	//	v, err := Unmarshal(b)
	//	if err != nil {
	//		return lazyerrors.Error(err)
	//	}
	//	if err = td.Set(key, v); err != nil {
	//		return lazyerrors.Error(err)
	//	}
	//}

	////-----------WORKING--------
	//td := types.MustMakeDocument()
	//fmt.Println("Loop:")
	//for key, value := range rawMessages {
	//	fmt.Println("Key:")
	//	fmt.Println(key)
	//	fmt.Println("value")
	//	fmt.Println(value)
	//	unValue, err := Unmarshal(value)
	//	if err != nil {
	//		return lazyerrors.Error(err)
	//	}
	//	fmt.Println("un_value")
	//	fmt.Println(unValue)
	//	if err = td.Set(key, unValue); err != nil {
	//		return lazyerrors.Error(err)
	//	}
	//}

	fmt.Println("td:")
	fmt.Println(td)
	*doc = Document(td)
	return nil
}

// MarshalJSON implements fjsontype interface.
func (doc *Document) MarshalJSON() ([]byte, error) {
	td := types.Document(*doc)
	//fmt.Println("marshal Document:")
	//fmt.Println(td)
	var buf bytes.Buffer

	buf.WriteString(`{"$k":`)
	b, err := json.Marshal(td.Keys())
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	//fmt.Println("b")
	//fmt.Println(b)
	fmt.Println("OYOYOYOY")
	buf.Write(b)

	for _, key := range td.Keys() {
		buf.WriteByte(',')
		//fmt.Println("key")
		//fmt.Println(key)
		if b, err = json.Marshal(key); err != nil {
			return nil, lazyerrors.Error(err)
		}
		//fmt.Println("second b")
		//fmt.Println(b)

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

		//fmt.Println("third b")
		//fmt.Println(b)

		buf.Write(b)
	}

	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func MarshalJSONHANA(doc types.Document) ([]byte, error) {
	fmt.Println("marshal Document:")
	fmt.Println(doc)
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
		//if key == "_id" {
		//	continue
		//}
		buf.WriteByte(',')
		//fmt.Println("key")
		//fmt.Println(key)
		if b, err = json.Marshal(key); err != nil {
			return nil, lazyerrors.Error(err)
		}
		fmt.Println("second b")
		fmt.Println(b)

		buf.Write(b)
		buf.WriteByte(':')

		value, err := doc.Get(key)
		fmt.Println(value)
		if err != nil {
			fmt.Println("ERROR")
			return nil, lazyerrors.Error(err)
		}

		switch value := value.(type) {
		case types.Document:
			b, err = MarshalHANA(value)
		default:
			fmt.Println("%T", value)
			fmt.Println(value)
			b, err = Marshal(value)
		}
		//b, err = Marshal(value)
		c := []byte{123, 34, 36, 111, 34}
		fmt.Println(c)
		res := bytes.Contains(b, c)
		if res {
			fmt.Println("YAY")
			cAdd := []byte{123, 34, 111, 105, 100, 34}
			b = append(cAdd, b[5:]...)
			fmt.Println(cAdd)
			//fmt.Println(bNew)
		}
		//fmt.Println(c)
		fmt.Println(b)
		//fmt.Println(b[0:5])
		////fmt.Println(b)
		////var bAdd []byte
		////bAdd := b[5 : len(b)-1]
		//fmt.Println(len(c))
		//i := len(c)
		//fmt.Println(i)
		//fmt.Println(len(c))
		//i = i - 1
		//fmt.Println(i)
		//fmt.Println(c[5:i])
		//cAdd := []byte{123, 34, 111, 105, 100, 34}
		//cAdd = append(cAdd, b[5:]...)

		//fmt.Println("cAdd")
		//fmt.Println(cAdd)

		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		buf.Write(b)
	}

	//-----WORKING------
	//buf.WriteByte('{')
	//i := 0
	//for _, key := range doc.Keys() {
	//	if key == "_id" {
	//		continue
	//	}
	//
	//	if i != 0 {
	//		buf.WriteByte(',')
	//	}
	//
	//	if b, err = json.Marshal(key); err != nil {
	//		return nil, lazyerrors.Error(err)
	//	}
	//
	//	buf.Write(b)
	//	buf.WriteByte(':')
	//
	//	value, err := doc.Get(key)
	//	if err != nil {
	//		return nil, lazyerrors.Error(err)
	//	}
	//
	//	b, err := Marshal(value)
	//	if err != nil {
	//		return nil, lazyerrors.Error(err)
	//	}
	//
	//	buf.Write(b)
	//	i = 1
	//}
	//-----WORKING------
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// check interfaces
var (
	_ fjsontype = (*Document)(nil)
)
