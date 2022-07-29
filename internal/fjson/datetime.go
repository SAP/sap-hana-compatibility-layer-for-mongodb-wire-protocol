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

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
)

// DateTime represents BSON DateTime data type.
type DateTime time.Time

// fjsontype implements fjsontype interface.
func (dt *DateTime) fjsontype() {}

// String returns formatted time for debugging.
func (dt *DateTime) String() string {
	return time.Time(*dt).Format(time.RFC3339Nano)
}

// $da should be changed if ever datetime is supported for storing
type dateTimeJSON struct {
	D int64 `json:"$da"`
}

// UnmarshalJSON implements fjsontype interface.
func (dt *DateTime) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		panic("null data")
	}

	r := bytes.NewReader(data)
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	var o dateTimeJSON
	if err := dec.Decode(&o); err != nil {
		return lazyerrors.Error(err)
	}
	if err := checkConsumed(dec, r); err != nil {
		return lazyerrors.Error(err)
	}

	// TODO Use .UTC()
	*dt = DateTime(time.UnixMilli(o.D))
	return nil
}

// MarshalJSON implements fjsontype interface.
func (dt *DateTime) MarshalJSON() ([]byte, error) {
	res, err := json.Marshal(dateTimeJSON{
		D: time.Time(*dt).UnixMilli(),
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	return res, nil
}

// check interfaces
var (
	_ fjsontype = (*DateTime)(nil)
)
