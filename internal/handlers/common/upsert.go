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

package common

import (
	"crypto/rand"
	"encoding/binary"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
)

func Upsert(updateDoc *types.Document, filter *types.Document, replace bool) (*types.Document, error) {
	var doc *types.Document
	var d *types.Document
	var err error

	if replace {
		doc = updateDoc
	} else {
		d, err = filterUpsert(filter)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
		doc, err = updateUpsert(updateDoc, d)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
	}

	if _, err := doc.Get("_id"); err != nil {
		objId := generateObjectID()
		doc.Set("_id", objId)
	}

	return doc, nil
}

func filterUpsert(filter *types.Document) (*types.Document, error) {
	doc := types.MustMakeDocument()

	for key, value := range filter.Map() {
		if strings.HasPrefix(key, "$") {
			continue
		}
		if _, ok := value.(types.Document); ok {
			continue
		}
		if _, ok := value.(types.Array); ok {
			continue
		}
		if strings.Contains(key, ".") {
			continue
		}

		err := doc.Set(key, value)
		if err != nil {
			return nil, err
		}
	}

	return &doc, nil
}

func updateUpsert(updateDoc *types.Document, d *types.Document) (*types.Document, error) {
	updateMap := updateDoc.Map()

	setDoc, ok := updateMap["$set"].(types.Document)
	if !ok {
		return d, nil
	}

	for key, value := range setDoc.Map() {
		if strings.HasPrefix(key, "$") {
			continue
		}
		if strings.Contains(key, ".") {
			continue
		}

		if dValue, err := d.Get(key); err == nil {

			if reflect.DeepEqual(value, dValue) {
				continue
			} else {
				return nil, lazyerrors.Errorf("Key-value pair %s:%s from query document is not equal to same key-value pair %s:%s in update document", key, dValue, key, value)
			}
		}

		err := d.Set(key, value)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

	}

	return d, nil
}

func generateObjectID() types.ObjectID {
	var res types.ObjectID
	t := time.Now()

	binary.BigEndian.PutUint32(res[0:4], uint32(t.Unix()))
	copy(res[4:9], objectIDProcess[:])

	c := atomic.AddUint32(&objectIDCounter, 1)

	// ignore the most significant byte for correct wraparound
	res[9] = byte(c >> 16)
	res[10] = byte(c >> 8)
	res[11] = byte(c)

	return res
}

var (
	objectIDProcess [5]byte
	objectIDCounter uint32
)

func init() {
	NotFail(io.ReadFull(rand.Reader, objectIDProcess[:]))
	NoError(binary.Read(rand.Reader, binary.BigEndian, &objectIDCounter))
}

func NotFail[T any](res T, err error) T {
	if err != nil {
		panic(err)
	}
	return res
}

func NoError(err error) {
	if err != nil {
		panic(err)
	}
}
