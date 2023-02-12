// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/stretchr/testify/assert"
)

type upserCase struct {
	caseName  string
	updateDoc *types.Document
	filter    *types.Document
	replace   bool
	e         upsertExpected
}

type upsertExpected struct {
	expDoc *types.Document
	expErr error
}

func TestUpsert(t *testing.T) {
	t.Parallel()

	upserCases := []upserCase{
		{
			caseName: "update with upsert", updateDoc: types.MustMakeDocumentPointer("$set", types.MustMakeDocument("name", "test", "type", "normal", "number", int32(123))),
			filter: types.MustMakeDocumentPointer("name", "test"), replace: false, e: upsertExpected{expDoc: types.MustMakeDocumentPointer("name", "test", "type", "normal", "number", int32(123)), expErr: nil},
		},
		{
			caseName: "replace with upsert", updateDoc: types.MustMakeDocumentPointer("type", "normal", "number", int32(123)),
			filter: types.MustMakeDocumentPointer("name", "test"), replace: true, e: upsertExpected{expDoc: types.MustMakeDocumentPointer("type", "normal", "number", int32(123)), expErr: nil},
		},
		{
			caseName: "update with upsert - update and filter with none equal key-value pair error", updateDoc: types.MustMakeDocumentPointer("$set", types.MustMakeDocument("name", "testing", "type", "normal", "number", int32(123))),
			filter: types.MustMakeDocumentPointer("name", "test"), replace: false, e: upsertExpected{expDoc: nil, expErr: fmt.Errorf("Key-value pair name:test from query document is not equal to same key-value pair name:testing in update document")},
		},
	}

	for _, field := range upserCases {
		actualDoc, actualErr := Upsert(field.updateDoc, field.filter, field.replace)

		if actualErr == nil {
			_, err := actualDoc.Get("_id")
			assert.NoError(t, err)
			actualDoc.Remove("_id")
		}

		if actualErr != nil && field.e.expErr != nil {
			if !strings.Contains(actualErr.Error(), field.e.expErr.Error()) {
				t.Errorf("%s: Upsert(%v, %v, %t) FAILED. Expected doc = %v and err = %v but got doc = %v and err = %v", field.caseName, field.updateDoc, field.filter, field.replace, field.e.expDoc, field.e.expErr, actualDoc, actualErr)
			}
		} else if actualErr == nil && field.e.expErr == nil {
			if actualDoc == nil && field.e.expDoc != nil || actualDoc != nil && field.e.expDoc == nil {
				t.Errorf("%s: Upsert(%v, %v, %t) FAILED. Expected doc = %v and err = %v but got doc = %v and err = %v", field.caseName, field.updateDoc, field.filter, field.replace, field.e.expDoc, field.e.expErr, actualDoc, actualErr)
			} else if !reflect.DeepEqual(field.e.expDoc.Map(), actualDoc.Map()) {
				t.Errorf("%s: Upsert(%v, %v, %t) FAILED. Expected doc = %v and err = %v but got doc = %v and err = %v", field.caseName, field.updateDoc, field.filter, field.replace, field.e.expDoc, field.e.expErr, actualDoc, actualErr)
			}
		} else {
			t.Errorf("%s: Upsert(%v, %v, %t) FAILED. Expected doc = %v and err = %v but got doc = %v and err = %v", field.caseName, field.updateDoc, field.filter, field.replace, field.e.expDoc, field.e.expErr, actualDoc, actualErr)
		}
	}
}
