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
	"github.com/DocStore/HANA_HWY/internal/types"
	"github.com/DocStore/HANA_HWY/internal/util/lazyerrors"
)

func isProjectionInclusion(projection types.Document) (inclusion bool, err error) {
	var exclusion bool
	for _, k := range projection.Keys() {
		if k == "_id" { // _id is a special case and can be both
			continue
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

					err = lazyerrors.Errorf("Cannot do inclusion on field #{k} in exclusion projection")
					return
				}
				inclusion = true
			} else {
				if inclusion {
					err = lazyerrors.Errorf("Cannot do exclusion on field #{k} in inclusion projection")
					return
				}
				exclusion = true
			}

		case int32, int64, float64:
			var equal types.CompareResult
			equal = 0
			if types.CompareScalars(v, int32(0)) == equal {
				if inclusion {
					err = lazyerrors.Errorf("Cannot do exclusion on field #{k} in inclusion projection")

					return
				}
				exclusion = true
			} else {
				if exclusion {
					err = lazyerrors.Errorf("Cannot do inclusion on field #{k} in exclusion projection")
					return
				}
				inclusion = true
			}
		default:
			err = lazyerrors.Errorf("unsupported operation %s %v (%T)", k, v, v)
			return
		}
	}
	return
}

func inclusionProjection(projection types.Document) (sql string) {
	keysSQL := "{\"ignoreKeys\": \"keys\", \"keys\": '[\"_id\", \"ignoreKeys\" "
	sql = ", \"_id\": \"_id\""

	for _, k := range projection.Keys() {

		if k == "_id" {
			continue
		}
		keysSQL += ", \"" + k + "\""
		sql += ", \"" + k + "\": \"" + k + "\""

	}

	keysSQL += "]'"
	sql = keysSQL + sql + "}"

	return
}

func Projection(projection types.Document) (sql string, exclusion bool, projectBool bool, err error) {
	unimplementedFields := []string{
		"$",
		"$elemMatch",
		"$meta",
		"$slice",
		"$comment",
		"$rand",
	}

	if err := Unimplemented(&projection, unimplementedFields...); err != nil {
		return "", false, false, err
	}

	projectionMap := projection.Map()
	if len(projectionMap) == 0 {
		sql = "*"
		return
	}

	projectBool = true

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

func ProjectDocuments(docs *types.Array, projection types.Document, exclusion bool) (err error) {
	for i := 0; i < docs.Len(); i++ {
		doc, errGet := docs.GetPointer(i)
		if errGet != nil {
			return errGet
		}
		switch docv := (*doc).(type) {
		case types.Document:
			err = projectDocument(&docv, projection, exclusion)
			*doc = docv
		default:
			err = lazyerrors.Errorf("Array contains a type not being types.Document")
		}
		if err != nil {
			return
		}
	}
	return nil
}

func projectDocument(doc *types.Document, projection types.Document, exclusion bool) (err error) {
	projectionMap := projection.Map()

	for k1 := range doc.Map() {
		projectionVal, ok := projectionMap[k1]

		if !ok {
			if k1 == "_id" { // if _id is not in projection map, do not do anything with it
				continue
			}
			if k1 == "ignoreKeys" {
				continue
			}
			if exclusion { // k1 from doc is absent in projection, remove from doc only if projection type inclusion
				continue
			}
			// inclusion
			doc.Remove(k1)
			continue
		}

		switch projectionVal := projectionVal.(type) { // found in the projection
		case bool: // field: bool
			if !projectionVal {
				doc.Remove(k1)
			} else { // inclusion
				docVal := (doc.Map())[k1]
				switch docVal.(type) {
				case nil:
					valType := (doc.Map())["ignoreKeys"]
					switch keys := valType.(type) {
					case *types.Array:
						if !keys.Contains(k1) {
							doc.Remove(k1)
						}
					}
				}
			}

		case int32, int64, float64: // field: number
			var equal types.CompareResult
			equal = 0
			if types.CompareScalars(projectionVal, int32(0)) == equal {
				doc.Remove(k1)
			} else { // inclusion
				docVal := (doc.Map())[k1]
				switch docVal.(type) {
				case nil:
					valType := (doc.Map())["ignoreKeys"]
					switch keys := valType.(type) {
					case *types.Array:
						if !keys.Contains(k1) {
							doc.Remove(k1)
						}
					}
				}
			}
		default:
			return lazyerrors.Errorf("unsupported projection operation %s %v (%T)", k1, projectionVal, projectionVal)
		}
	}

	if !exclusion {
		doc.Remove("ignoreKeys")
	}
	return nil
}
