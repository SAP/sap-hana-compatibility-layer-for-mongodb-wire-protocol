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
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// ObjectID represents BSON ObjectID data type.
type ObjectID types.ObjectID

// fjsontype implements fjsontype interface.
func (obj *ObjectID) fjsontype() {}

type objectIDJSON struct {
	O string `json:"oid"`
}

// UnmarshalJSON implements fjsontype interface.
func (obj *ObjectID) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		panic("null data")
	}
	fmt.Println("data")
	fmt.Println(data)
	r := bytes.NewReader(data)
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	var o objectIDJSON
	if err := dec.Decode(&o); err != nil {
		fmt.Println("here1")
		return lazyerrors.Error(err)
	}
	fmt.Println(o)
	if err := checkConsumed(dec, r); err != nil {
		fmt.Println("here2")
		return lazyerrors.Error(err)
	}
	fmt.Println(o)
	b, err := hex.DecodeString(o.O)
	if err != nil {
		fmt.Println("here3")
		return lazyerrors.Error(err)
	}
	fmt.Println(o)
	if len(b) != 12 {
		fmt.Println("here4")
		return lazyerrors.Errorf("fjson.ObjectID.UnmarshalJSON: %d bytes", len(b))
	}
	fmt.Println(o)
	copy(obj[:], b)

	return nil
}

// MarshalJSON implements fjsontype interface.
func (obj *ObjectID) MarshalJSON() ([]byte, error) {
	fmt.Println("obj")
	fmt.Println(obj)
	fmt.Println(obj[:])
	fmt.Println(hex.EncodeToString(obj[:]))
	byt := make([]byte, hex.EncodedLen(len(obj[:])))
	i := hex.Encode(byt, obj[:])
	fmt.Println(i)
	fmt.Println(byt)
	res, err := json.Marshal(objectIDJSON{
		O: hex.EncodeToString(obj[:]),
	})
	fmt.Println("res")
	fmt.Println(res)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	return res, nil
}

// MarshalJSONObjectHANA implements fjsontype interface.
func MarshalJSONObjectHANA(obj types.ObjectID) ([]byte, error) {
	fmt.Println("obj")
	fmt.Println(obj)
	fmt.Println(obj[:])
	fmt.Println(hex.EncodeToString(obj[:]))
	byt := make([]byte, hex.EncodedLen(len(obj[:])))
	i := hex.Encode(byt, obj[:])
	fmt.Println(i)
	fmt.Println(byt)
	byt = append([]byte{39}, byt...)
	byt = append(byt, []byte{39, 125}...)
	fmt.Println(byt)
	//
	//res, err := json.Marshal(objectIDJSON{
	//	O: string(byt),
	//})
	res := append([]byte{123, 34, 111, 105, 100, 34, 58, 32}, byt...)
	fmt.Println("res")
	fmt.Println(res)
	//if err != nil {
	//	return nil, lazyerrors.Error(err)
	//}
	return res, nil
}

// check interfaces
var (
	_ fjsontype = (*ObjectID)(nil)
)
