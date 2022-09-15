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
	"strconv"
	"strings"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
)

// Projection checks if projection is an inclusion or exclusion.
// If inclusion then the sql needed to perform inclusion is created.
// If exclusion then the removal of fields will first happen after retrieval of documents.
func Projection(projection types.Document) (sql string, exclusion bool, err error) {
	unimplementedFields := []string{
		"$",
		"$elemMatch",
		"$meta",
		"$slice",
		"$comment",
		"$rand",
	}

	if err = Unimplemented(&projection, unimplementedFields...); err != nil {
		return
	}

	projectionMap := projection.Map()
	if len(projectionMap) == 0 {
		sql = "*"
		return
	}

	inclusion, err := isProjectionInclusion(projection)
	if err != nil {
		return
	}

	if inclusion {
		sql = inclusionProjection(projection)
		return
	} else {
		exclusion = true
		sql = "*"
		return
	}
}

// isProjectionInclusion determines whether projection is inclusion or exclusion.
func isProjectionInclusion(projection types.Document) (inclusion bool, err error) {
	var exclusion bool
	for _, k := range projection.Keys() {
		if k == "_id" { // _id is a special case where mixing exclusion and inclusion is allowed
			var v any
			v, err = projection.Get(k)
			switch v := v.(type) {
			case bool, int32, int64, float64:
				if len(projection.Map()) != 1 {
					continue
				}
			default:
				err = lazyerrors.Errorf("unsupported operation %s %v (%T)", k, v, v)
				return
			}
		}

		var v any
		v, err = projection.Get(k)
		if err != nil {
			err = lazyerrors.Errorf("no value for %s.", k)
			return
		}
		switch v := v.(type) {
		case bool:
			if v {
				if exclusion {
					err = NewErrorMessage(ErrProjectionInEx, "Cannot do inclusion on field %s in exclusion projection", k)
					return
				}
				if strings.Contains(k, ".") {
					err = lazyerrors.Errorf("Projection on nested documents is not implemented, yet.")
					return
				}
				inclusion = true
			} else {
				if inclusion {
					err = NewErrorMessage(ErrProjectionExIn, "Cannot do exclusion on field %s in inclusion projection", k)
					return
				}
				exclusion = true
			}

		case int32, int64, float64:
			var equal types.CompareResult
			equal = 0
			if types.CompareScalars(v, int32(0)) == equal {
				if inclusion {
					err = NewErrorMessage(ErrProjectionExIn, "Cannot do exclusion on field %s in inclusion projection", k)
					return
				}
				exclusion = true
			} else {
				if exclusion {
					err = NewErrorMessage(ErrProjectionInEx, "Cannot do inclusion on field %s in exclusion projection", k)
					return
				}
				if strings.Contains(k, ".") {
					err = lazyerrors.Errorf("Projection on nested documents is not implemented, yet.")
					return
				}
				inclusion = true
			}
		default:
			err = lazyerrors.Errorf("Only $set and $unset are supported for update operations")
			return
		}
	}
	return
}

// inclusionProjection prepares the SQL statement for inclusion. This is using the json projection
func inclusionProjection(projection types.Document) (sql string) {
	sql = "{"
	if id, err := projection.Get("_id"); err == nil {
		switch id := id.(type) {
		case bool:
			if id {
				if len(projection.Map()) == 1 {
					sql += "\"_id\": \"_id\"}"
					return
				}
				sql += "\"_id\": \"_id\", "
			}
		case int32, int64, float64:
			var equal types.CompareResult
			equal = 0
			if types.CompareScalars(id, int32(0)) != equal {
				if len(projection.Map()) == 1 {
					sql += "\"_id\": \"_id\"}"
					return
				}
				sql += "\"_id\": \"_id\", "
			}
		}
	} else {
		sql += "\"_id\": \"_id\", "
	}

	for i, k := range projection.Keys() {

		if k == "_id" {
			continue
		}

		if i != 0 {
			sql += ", "
		}

		sql += "\"" + k + "\": \"" + k + "\""

	}

	sql += "}"

	return
}

// ProjectDocuments will be used if it is an exclusion to performs the exclusion
// on each document together with the function projectDocument
func ProjectDocuments(docs *types.Array, projection types.Document) (err error) {
	for i := 0; i < docs.Len(); i++ {
		doc, errGet := docs.GetPointer(i)
		if errGet != nil {
			return errGet
		}
		switch docv := (*doc).(type) {
		case types.Document:
			err = projectDocument(&docv, projection)
			*doc = docv
		default:
			err = lazyerrors.Errorf("Array of retrieved documents contains a type not being types.Document")
		}
		if err != nil {
			return
		}
	}
	return nil
}

// projectDocument removes the fields of a document specified in the exclusion
func projectDocument(doc *types.Document, projection types.Document) (err error) {
	projectionMap := projection.Map()
	for field := range projectionMap {
		if strings.Contains(field, ".") {
			var next any = doc
			var previousS string
			var previousDoc types.Document
			var previousArray *types.Array
			var ppDoc types.Document
			var ppS string
			var notFound bool
			var projErr error
			arrayCount := 0
		forLoop:
			for _, s := range strings.Split(field, ".") {
				switch j := next.(type) {
				case *types.Document:
					previousDoc = *j
					previousS = s
					next, projErr = j.Get(s)
					if projErr != nil || next == nil {
						notFound = true
						break forLoop
					}
				case types.Document:
					ppDoc = previousDoc
					ppS = previousS
					previousS = s
					previousDoc = j
					next, projErr = j.Get(s)
					if projErr != nil || next == nil {
						notFound = true
						break forLoop
					}
					if arrayCount > 0 {
						arrayCount--
					}
				case *types.Array:
					ppDoc = previousDoc
					ppS = previousS
					previousS = s
					previousArray = j
					if sInt, convErr := strconv.Atoi(s); convErr == nil {
						next, projErr = j.Get(sInt)
					} else {
						notFound = true
						break forLoop
					}

					if projErr != nil {
						notFound = true
						break forLoop
					}
					arrayCount = 2
				default:
					notFound = true
					continue
				}
			}
			if notFound {
				continue
			}
			if arrayCount == 0 {
				previousDoc.Remove(previousS)
				ppDoc.Set(ppS, previousDoc)
			} else if arrayCount == 1 {
				previousDoc.Remove(previousS)
				sInt, _ := strconv.Atoi(ppS)
				previousArray.Set(sInt, previousDoc)
			} else if arrayCount == 2 {
				sInt, _ := strconv.Atoi(previousS)
				previousArray.Delete(sInt)
			}

		} else {
			if field == "_id" {
				idExclusion := projectionMap[field]
				switch idExclusion := idExclusion.(type) {
				case bool:
					if !idExclusion {
						doc.Remove(field)
					}
					continue
				case int32, int64, float64:
					var equal types.CompareResult
					equal = 0
					if types.CompareScalars(idExclusion, int32(0)) == equal {
						doc.Remove(field)
					}
					continue
				}
			}
			doc.Remove(field)
		}
	}

	return nil
}
