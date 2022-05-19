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

	"github.com/lucboj/FerretDB_SAP_HANA/internal/types"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/util/lazyerrors"
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

	r := bytes.NewReader(data)
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	var o objectIDJSON
	if err := dec.Decode(&o); err != nil {
		return lazyerrors.Error(err)
	}

	if err := checkConsumed(dec, r); err != nil {
		return lazyerrors.Error(err)
	}

	b, err := hex.DecodeString(o.O)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if len(b) != 12 {
		return lazyerrors.Errorf("fjson.ObjectID.UnmarshalJSON: %d bytes", len(b))
	}

	copy(obj[:], b)

	return nil
}

// MarshalJSON implements fjsontype interface.
func (obj *ObjectID) MarshalJSON() ([]byte, error) {

	res, err := json.Marshal(objectIDJSON{
		O: hex.EncodeToString(obj[:]),
	})

	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	return res, nil
}

// MarshalJSONObjectHANA implements fjsontype interface.
func MarshalJSONObjectHANA(obj types.ObjectID) ([]byte, error) {

	byt := make([]byte, hex.EncodedLen(len(obj[:])))
	byt = append([]byte{39}, byt...)
	byt = append(byt, []byte{39, 125}...)

	res := append([]byte{123, 34, 111, 105, 100, 34, 58, 32}, byt...)

	return res, nil
}

// check interfaces
var (
	_ fjsontype = (*ObjectID)(nil)
)
