// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"fmt"
	"strings"
	"testing"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
)

type testCase struct {
	name string
	r    types.Document
	e    expected
}

type expected struct {
	sql       string
	exclusion bool
	inclusion bool
	err       error
}

func TestProjection(t *testing.T) {

	projectionTestCases := []testCase{
		{name: "projection exclusion test", r: types.MustMakeDocument("field", false),
			e: expected{sql: "*", exclusion: true, err: nil}},
		{name: "inclusion nested document error test", r: types.MustMakeDocument("field.nest", true),
			e: expected{sql: "", exclusion: false, err: fmt.Errorf("Projection on nested documents is not implemented, yet.")}},
		{name: "empty projection document test", r: types.MustMakeDocument(),
			e: expected{sql: "*", exclusion: false, err: nil}},
		{name: "projection inclusion test", r: types.MustMakeDocument("field", true),
			e: expected{sql: "{\"_id\": \"_id\", \"field\": \"field\"}", exclusion: false, err: nil}},
		{name: "unimplemented operation error test", r: types.MustMakeDocument("$elemMatch", true),
			e: expected{sql: "", exclusion: false, err: fmt.Errorf("NotImplemented (238): $elemmatch: support for field \"$elemMatch\" is not implemented yet")}},
	}

	for _, field := range projectionTestCases {

		sql, exclusion, err := Projection(field.r)

		if field.e.err != nil {
			if !strings.EqualFold(sql, field.e.sql) || exclusion != field.e.exclusion || !strings.Contains(err.Error(), field.e.err.Error()) {
				t.Errorf("%s: Projection(%v) FAILED. Expected sql = %s, exclusion = %v and err = %v got sql = %s, exclusion = %v and err = %v", field.name,
					field.r, field.e.sql, field.e.exclusion, field.e.err, sql, exclusion, err)
			}
		} else {
			if !strings.EqualFold(sql, field.e.sql) || exclusion != field.e.exclusion || err != field.e.err {
				t.Errorf("%s: Projection(%v) FAILED. Expected sql = %s, exclusion = %v and err = %v got sql = %s, exclusion = %v and err = %v", field.name,
					field.r, field.e.sql, field.e.exclusion, field.e.err, sql, exclusion, err)
			}
		}

	}
}

func TestIsProjectionInclusion(t *testing.T) {

	isProjectionInclusionTestCases := []testCase{
		{name: "inclusion bool test", r: types.MustMakeDocument("field", true),
			e: expected{inclusion: true, err: nil}},
		{name: "inclusion number test", r: types.MustMakeDocument("field", int64(1)),
			e: expected{inclusion: true, err: nil}},
		{name: "exclusion bool test", r: types.MustMakeDocument("field", false),
			e: expected{inclusion: false, err: nil}},
		{name: "exclusion number test", r: types.MustMakeDocument("field", float64(0)),
			e: expected{inclusion: false, err: nil}},
		{name: "specialcase inclusion excluding _id test", r: types.MustMakeDocument("field", int32(1), "_id", false),
			e: expected{inclusion: true, err: nil}},
		{name: "inclusion nested document error test", r: types.MustMakeDocument("field.nest", int32(1)),
			e: expected{inclusion: false, err: fmt.Errorf("Projection on nested documents is not implemented, yet.")}},
	}

	for _, field := range isProjectionInclusionTestCases {
		inclusion, err := isProjectionInclusion(field.r)

		if field.e.err != nil {
			if inclusion != field.e.inclusion || !strings.Contains(err.Error(), field.e.err.Error()) {
				t.Errorf("%s: Projection(%v) FAILED. Expected inclusion = %v and err = %v got inclusion = %v and err = %v", field.name,
					field.r, field.e.inclusion, field.e.err, inclusion, err)
			}
		} else {
			if inclusion != field.e.inclusion || err != field.e.err {
				t.Errorf("%s: Projection(%v) FAILED. Expected inclusion = %v and err = %v got inclusion = %v and err = %v", field.name,
					field.r, field.e.inclusion, field.e.err, inclusion, err)
			}
		}
	}

}

func TestInclusionProjection(t *testing.T) {

	inclusionProjectionTestCases := []testCase{
		{name: "include fields test", r: types.MustMakeDocument("field1", int32(1), "field2", true, "field3", float64(-1.2)),
			e: expected{sql: "{\"_id\": \"_id\", \"field1\": \"field1\", \"field2\": \"field2\", \"field3\": \"field3\"}"}},
		{name: "include _id only number test", r: types.MustMakeDocument("_id", float64(23.21)),
			e: expected{sql: "{\"_id\": \"_id\"}"}},
		{name: "include fields and _id number test", r: types.MustMakeDocument("field1", int32(1), "_id", float64(23.21)),
			e: expected{sql: "{\"_id\": \"_id\", \"field1\": \"field1\"}"}},
		{name: "include _id only bool test", r: types.MustMakeDocument("_id", true),
			e: expected{sql: "{\"_id\": \"_id\"}"}},
		{name: "include fields and _id bool test", r: types.MustMakeDocument("field1", int32(1), "_id", true),
			e: expected{sql: "{\"_id\": \"_id\", \"field1\": \"field1\"}"}},
	}

	for _, field := range inclusionProjectionTestCases {
		sql := inclusionProjection(field.r)

		if field.e.err != nil {
			if !strings.EqualFold(sql, field.e.sql) {
				t.Errorf("%s: inclusionProjection(%v) FAILED. Expected sql = %s got sql = %s", field.name,
					field.r, field.e.sql, sql)
			}
		} else {
			if !strings.EqualFold(sql, field.e.sql) {
				t.Errorf("%s: inclusionProjection(%v) FAILED. Expected sql = %s got sql = %s", field.name,
					field.r, field.e.sql, sql)
			}
		}

	}
}

type testCaseProjectDocuments struct {
	name string
	r1   *types.Array
	r2   types.Document
	e    exceptedProjDoc
}

type exceptedProjDoc struct {
	err  error
	eDoc types.Document
}

func TestProjectDocuments(t *testing.T) {

	projectDocumentsTestCases := []testCaseProjectDocuments{
		{name: "exclusion on document test", r1: types.MustNewArray(types.MustMakeDocument("_id", int32(1), "field", "string")), r2: types.MustMakeDocument("_id", int64(0), "field", false),
			e: exceptedProjDoc{err: nil, eDoc: types.MustMakeDocument()}},
		{name: "exclusion on document test", r1: types.MustNewArray(types.MustMakeDocument("_id", int32(1), "field", "string"), "string"), r2: types.MustMakeDocument("_id", int64(0), "field", false),
			e: exceptedProjDoc{err: fmt.Errorf("Array contains a type not being types.Document"), eDoc: types.MustMakeDocument()}},
	}

	for _, field := range projectDocumentsTestCases {
		err := ProjectDocuments(field.r1, field.r2)
		gotDoc, docErr := field.r1.Get(0)
		if docErr != nil {
			t.Error(docErr)
		}

		if field.e.err != nil {
			if !strings.Contains(err.Error(), field.e.err.Error()) || !strings.EqualFold(fmt.Sprintf("%v", gotDoc.(types.Document)), fmt.Sprintf("%v", field.e.eDoc)) {
				t.Errorf("%s: ProjectionDocuments(%v, %v) FAILED. Expected doc_in_array = %v and err = %v got doc_in_array = %v and err = %v", field.name,
					field.r1, field.r2, field.e.eDoc, field.e.err, gotDoc, err)
			}
		} else {
			if err != field.e.err || !strings.EqualFold(fmt.Sprintf("%v", gotDoc.(types.Document)), fmt.Sprintf("%v", field.e.eDoc)) {
				t.Errorf("%s: ProjectionDocuments(%v, %v) FAILED. Expected doc_in_array = %v and err = %v got doc_in_array = %v and err = %v", field.name,
					field.r1, field.r2, field.e.eDoc, field.e.err, gotDoc, err)
			}
		}

	}
}

type testCaseProjectDocument struct {
	name string
	r1   types.Document
	r2   types.Document
	e    exceptedProjDoc
}

func TestProjectionDocument(t *testing.T) {

	projectDocumentTestCases := []testCaseProjectDocument{
		{name: "exclude fields test", r1: types.MustMakeDocument("field", int32(123)), r2: types.MustMakeDocument("field", false),
			e: exceptedProjDoc{err: nil, eDoc: types.MustMakeDocument()}},
		{name: "exclude from nested array test", r1: types.MustMakeDocument("field1", types.MustMakeDocument("field2", types.MustNewArray(types.MustMakeDocument("field3", types.MustNewArray(int32(1), int32(2), types.MustNewArray("disappears", "stays"), int32(4), int32(5))), int32(3)))), r2: types.MustMakeDocument("field1.field2.0.field3.2.0", false),
			e: exceptedProjDoc{err: nil, eDoc: types.MustMakeDocument("field1", types.MustMakeDocument("field2", types.MustNewArray(types.MustMakeDocument("field3", types.MustNewArray(int32(1), int32(2), types.MustNewArray("stays"), int32(4), int32(5))), int32(3))))}},
	}

outer_loop:
	for _, field := range projectDocumentTestCases {
		err := projectDocument(&field.r1, field.r2)

		if field.name == "exclude from nested array test" {
			path := []any{"field1", "field2", 0, "field3", 2, 0}

			var got any
			var expected any
			for i, e := range path {
				if i == 0 {
					if eStr, ok := e.(string); ok {
						got, _ = field.r1.Get(eStr)
						expected, _ = field.e.eDoc.Get(eStr)
					}
					continue
				}
				if eStr, ok := e.(string); ok {
					got, _ = got.(types.Document).Get(eStr)
					expected, _ = expected.(types.Document).Get(eStr)
				} else {
					eInt := e.(int)
					got, _ = got.(*types.Array).Get(eInt)
					expected, _ = expected.(*types.Array).Get(eInt)
				}

			}

			if !strings.EqualFold(got.(string), expected.(string)) || err != field.e.err {
				t.Errorf("%s: ProjectionDocuments(%v, %v) FAILED. Expected elem_in_array = %v and err = %v got doc_in_array = %v and err = %v", field.name,
					field.r1, field.r2, expected, field.e.err, got, err)
			}
			continue outer_loop
		}

		if field.e.err != nil {
			if !strings.Contains(err.Error(), field.e.err.Error()) || !strings.EqualFold(fmt.Sprintf("%v", field.r1), fmt.Sprintf("%v", field.e.eDoc)) {
				t.Errorf("%s: ProjectionDocuments(%v, %v) FAILED. Expected doc_in_array = %v and err = %v got doc_in_array = %v and err = %v", field.name,
					field.r1, field.r2, field.e.eDoc, field.e.err, field.r1, err)
			}
		} else {
			if err != field.e.err || !strings.EqualFold(fmt.Sprintf("%v", field.r1), fmt.Sprintf("%v", field.e.eDoc)) {
				t.Errorf("%s: ProjectionDocuments(%v, %v) FAILED. Expected doc_in_array = %v and err = %v got doc_in_array = %v and err = %v", field.name,
					field.r1, field.r2, field.e.eDoc, field.e.err, field.r1, err)
			}
		}
	}
}
