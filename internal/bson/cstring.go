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

package bson

import (
	"bufio"

	"github.com/DocStore/HANA_HWY/internal/fjson"
	"github.com/DocStore/HANA_HWY/internal/util/lazyerrors"
)

// CString represents BSON CString data type.
type CString string

func (cstr *CString) bsontype() {}

// ReadFrom implements bsontype interface.
func (cstr *CString) ReadFrom(r *bufio.Reader) error {
	b, err := r.ReadBytes(0)
	if err != nil {
		return lazyerrors.Errorf("bson.CString.ReadFrom: %w", err)
	}

	*cstr = CString(b[:len(b)-1])
	return nil
}

// WriteTo implements bsontype interface.
func (cstr CString) WriteTo(w *bufio.Writer) error {
	v, err := cstr.MarshalBinary()
	if err != nil {
		return lazyerrors.Errorf("bson.CString.WriteTo: %w", err)
	}

	_, err = w.Write(v)
	if err != nil {
		return lazyerrors.Errorf("bson.CString.WriteTo: %w", err)
	}

	return nil
}

// MarshalBinary implements bsontype interface.
func (cstr CString) MarshalBinary() ([]byte, error) {
	b := make([]byte, len(cstr)+1)
	copy(b, cstr)
	return b, nil
}

// UnmarshalJSON implements bsontype interface.
func (cstr *CString) UnmarshalJSON(data []byte) error {
	// 	var cstrJ fjson.CString
	// 	if err := cstrJ.UnmarshalJSON(data); err != nil {
	// 		return err
	// 	}

	// 	*cstr = CString(cstrJ)
	return nil
}

// MarshalJSON implements bsontype interface.
func (cstr CString) MarshalJSON() ([]byte, error) {
	return fjson.Marshal(fromBSON(&cstr))
}

// check interfaces
var (
	_ bsontype = (*CString)(nil)
)
