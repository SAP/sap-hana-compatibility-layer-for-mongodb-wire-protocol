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

// Package fjson provides converters from/to FJSON.
//
// All BSON data types have three representations in SAP HANA compatibility layer for MongoDB Wire Protocol:
//
//  1. As they are used in handlers implementation (types package).
//  2. As they are used in the wire protocol implementation (bson package).
//  3. As they are used to store data in PostgreSQL (fjson package).
//
// The reason for that is a separation of concerns: to avoid method names clashes, to simplify type asserts, etc.
//
// JSON mapping for storage
//
// Composite/pointer types
//  Document:   {"$k": ["<key 1>", "<key 2>", ...], "<key 1>": <value 1>, "<key 2>": <value 2>, ...}
//  Array:      JSON array
// Scalar/value types
//  Double:     {"tf": JSON number} or {"tf": "Infinity|-Infinity|NaN"}
//  String:     JSON string
//  Binary:     {"$b": "<base 64 string>", "s": <subtype number>}
//  ObjectID:   {"$o": "<ObjectID as 24 character hex string"}
//  Bool:       JSON true / false values
//  DateTime:   {"$d": milliseconds since epoch as JSON number}
//  nil:        JSON null
//  Regex:      {"$r": "<string without terminating 0x0>", "o": "<string without terminating 0x0>"}
//  Int32:      JSON number
//  Timestamp:  {"ts": "<number as string>"}
//  Int64:      {"$l": "<number as string>"}
//  Decimal128: {"$n": "<number as string>"}
//  CString:    {"$c": "<string without terminating 0x0>"}
package fjson

import (
	"bytes"
	"encoding/json"
	"io"
	"time"

	"github.com/AlekSi/pointer"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
)

type fjsontype interface {
	fjsontype() // seal for go-sumtype

	json.Unmarshaler
	json.Marshaler
}

//go-sumtype:decl fjsontype

// checkConsumed returns error if decoder or reader have buffered or unread data.
func checkConsumed(dec *json.Decoder, r *bytes.Reader) error {
	if dr := dec.Buffered().(*bytes.Reader); dr.Len() != 0 {
		b, _ := io.ReadAll(dr)
		return lazyerrors.Errorf("%d bytes remains in the decoded: %s", dr.Len(), b)
	}

	if l := r.Len(); l != 0 {
		b, _ := io.ReadAll(r)
		return lazyerrors.Errorf("%d bytes remains in the reader: %s", l, b)
	}

	return nil
}

// Everything commented out is at the moment not supported.
func fromFJSON(v fjsontype) any {
	switch v := v.(type) {
	case *Document:
		return types.Document(*v)
	case *Array:
		return pointer.To(types.Array(*v))
	case *Double:
		return float64(*v)
	case *String:
		return string(*v)
	// case *Binary:
	// 	return types.Binary(*v)
	case *ObjectID:
		return types.ObjectID(*v)
	case *Bool:
		return bool(*v)
	case *DateTime:
		return time.Time(*v)
	case nil:
		return nil
	case *Regex:
		return types.Regex(*v)
	case *Int64:
		return int64(*v)
	case *Int32:
		return int32(*v)
		// case *Timestamp:
		// 	return types.Timestamp(*v)
		// case *CString:
		// 	return types.CString(*v)
	}
	panic("not reached") // for go-sumtype to work
}

// Used for wire protocol.
func toFJSON(v any) fjsontype {
	switch v := v.(type) {
	case types.Document:
		return pointer.To(Document(v))
	case *types.Array:
		return pointer.To(Array(*v))
	case float64:
		return pointer.To(Double(v))
	case string:
		return pointer.To(String(v))
	// case types.Binary:
	// 	return pointer.To(Binary(v))
	case types.ObjectID:
		return pointer.To(ObjectID(v))
	case bool:
		return pointer.To(Bool(v))
	case time.Time:
		return pointer.To(DateTime(v))
	case nil:
		return nil
	case types.Regex:
		return pointer.To(Regex(v))
	case int64:
		return pointer.To(Int64(v))
	case int32:
		return pointer.To(Int64(v))
		// case types.Timestamp:
		// 	return pointer.To(Timestamp(v))
		// case types.CString:
		// 	return pointer.To(CString(v))
	}
	panic("not reached")
}

// used for MongoDB operations.
func toFJSONHANA(v any) (fjsontype, error) {
	switch v := v.(type) {
	case types.Document:
		return pointer.To(Document(v)), nil
	case *types.Array:
		return pointer.To(Array(*v)), nil
	case float64:
		return pointer.To(Double(v)), nil
	case string:
		return pointer.To(String(v)), nil
	case types.ObjectID:
		return pointer.To(ObjectID(v)), nil
	case bool:
		return pointer.To(Bool(v)), nil
	case nil:
		return nil, nil
	case int64:
		return pointer.To(Int64(v)), nil
	case int32:
		return pointer.To(Int64(v)), nil
	default:
		return nil, lazyerrors.Errorf("Datatype %T not supported", v)
	}
}

// Unmarshal decodes the given fjson-encoded data.
// Everything commented is at the moment not supported.
func Unmarshal(data []byte) (any, error) {
	var v any
	r := bytes.NewReader(data)
	dec := json.NewDecoder(r)
	err := dec.Decode(&v)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	if err := checkConsumed(dec, r); err != nil {
		return nil, lazyerrors.Error(err)
	}

	var res fjsontype
	switch v := v.(type) {
	case map[string]any:
		switch {
		// case v["ft"] != nil:
		// 	var o Double
		// 	err = o.UnmarshalJSON(data)
		// 	res = &o
		// 	res = &o
		// case v["$b"] != nil:
		// 	var o Binary
		// 	err = o.UnmarshalJSON(data)
		// 	res = &o
		case v["oid"] != nil:
			var o ObjectID
			err = o.UnmarshalJSON(data)
			res = &o
		case v["$da"] != nil:
			var o DateTime
			err = o.UnmarshalJSON(data)
			res = &o
		case v["$r"] != nil:
			var o Regex
			err = o.UnmarshalJSON(data)
			res = &o
		// case v["ts"] != nil:
		// 	var o Timestamp
		// 	err = o.UnmarshalJSON(data)
		// 	res = &o
		// case v["nl"] != nil:
		// 	fmt.Println("fjson")
		// 	fmt.Println(v)
		// 	var o Int64
		// 	err = o.UnmarshalJSON(data)
		// 	res = &o
		// case v["$c"] != nil:
		// 	var o CString
		// 	err = o.UnmarshalJSON(data)
		// 	res = &o
		default:
			var o Document
			err = o.UnmarshalJSON(data)
			res = &o
		}
	case string:
		res = pointer.To(String(v))
	case []any:
		var o Array
		err = o.UnmarshalJSON(data)
		res = &o
	case bool:
		res = pointer.To(Bool(v))
	case nil:
		res = nil
	case float64:
		vType, err := decoderNumber(data)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
		switch vType := vType.(type) {
		case int32:
			res = pointer.To(Int32(vType))
		case int64:
			res = pointer.To(Int64(vType))
		case float64:
			res = pointer.To(Double(vType))
		}

	default:
		err = lazyerrors.Errorf("fjson.Unmarshal: unhandled element %[1]T (%[1]v)", v)
	}

	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return fromFJSON(res), nil
}

// Marshal encodes given value into fjson. Used for wire protocol.
func Marshal(v any) ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}

	b, err := toFJSON(v).MarshalJSON()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return b, nil
}

// Marshal encodes given value into fjson. Used for MongoDB operations.
func MarshalHANA(v any) ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}

	f, err := toFJSONHANA(v)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	var b []byte
	switch f := f.(type) {
	case *Document:
		b, err = f.MarshalJSONHANA()
	default:
		b, err = f.MarshalJSON()
	}

	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return b, nil
}

// Needed since the JSON package returns only numbers as Float64.
func decoderNumber(data []byte) (any, error) {
	var err error = nil
	var num any
	d := json.NewDecoder(bytes.NewBuffer(data))
	d.UseNumber()
	if err := d.Decode(&num); err != nil {
		panic(err)
	}

	switch num := num.(type) {
	case json.Number:
		if _, err := num.Int64(); err != nil {
			return num.Float64()
		}
		numInt64, _ := num.Int64()
		if numInt64 > 2147483647 || numInt64 < -2147483648 {
			return numInt64, nil
		}
		return int32(numInt64), nil

	default:
		err = lazyerrors.Errorf("Not a json.Number")
	}
	return true, err
}
