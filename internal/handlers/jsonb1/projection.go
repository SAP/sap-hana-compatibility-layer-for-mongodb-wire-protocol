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

package jsonb1

import (
	"fmt"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
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
			//if compareScalars(v, int32(0)) == equal {
			//	if inclusion {
			//		err = lazyerrors.Errorf("Cannot do exclusion on field #{k} in inclusion projection")
			//
			//		return
			//	}
			//	exclusion = true
			//} else {
			//	if exclusion {
			//		err = lazyerrors.Errorf("Cannot do inclusion on field #{k} in exclusion projection")
			//		return
			//	}
			//	inclusion = true
			//}
			err = lazyerrors.Errorf("Is int32, int64, float64")
			return

		//case *types.Document:
		//	for _, projectionType := range v.Keys() {
		//		supportedProjectionTypes := []string{"$elemMatch"}
		//		if !slices.Contains(supportedProjectionTypes, projectionType) {
		//			err = lazyerrors.Errorf("projecion of %s is not supported", projectionType)
		//			return
		//		}
		//
		//		switch projectionType {
		//		case "$elemMatch":
		//			inclusion = true
		//		default:
		//			panic(projectionType + " not supported")
		//		}
		//	}
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

		keysSQL += ", \"" + k + "\""
		sql += ", \"" + k + "\": \"" + k + "\""
		fmt.Println("sql so far")
		fmt.Println(sql)

	}

	keysSQL += "]'"
	sql = keysSQL + sql + "}"
	fmt.Println("finished projection SQL")
	fmt.Println(sql)

	return
}

func projection(projection types.Document) (sql string, exclusion bool, projectBool bool, err error) {
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
	//
	//keysSQL := "{\"keys\": '[\"_id\" "
	//sql = ", \"_id\": \"_id\""
	//
	////for i, k := range projection.Keys() {
	////	if i != 0 {
	////		ks += ", "
	////	}
	////	ks += p.Next()
	////	args = append(args, k)
	////}
	//
	//for _, k := range projection.Keys() {
	//
	//	keysSQL += ", \"" + k + "\""
	//	sql += ", \"" + k + "\": \"" + k + "\""
	//	fmt.Println("sql so far")
	//	fmt.Println(sql)
	//
	//}
	//
	////sql = "json_build_object('$k', array[" + ks + "],"
	////for i, k := range projection.Keys() {
	////	if i != 0 {
	////		sql += ", "
	////	}
	////	sql += p.Next() + "::text, _jsonb->" + p.Next()
	////	args = append(args, k, k)
	////}
	//keysSQL += "]'"
	//sql = keysSQL + sql + "}"
	//fmt.Println("finished projection SQL")
	//fmt.Println(sql)

}

func projectDocuments(docs *types.Array, projection types.Document, exclusion bool) (err error) {
	fmt.Println("PROJECTIONDOCUMENTS1")

	for i := 0; i < docs.Len(); i++ {
		doc, errGet := docs.GetPointer(i)
		fmt.Println("PROJECTIONDOCUMENTS2")
		fmt.Println(doc)
		fmt.Println(&doc)
		fmt.Println("HERE")
		fmt.Println(*doc)
		if errGet != nil {
			return errGet
		}
		switch docv := (*doc).(type) {
		case types.Document:

			err = projectDocument(&docv, projection, exclusion)
			*doc = docv

			fmt.Println("newwer DOC")
			fmt.Println(doc)
			fmt.Println(&doc)
		default:
			err = lazyerrors.Errorf("Array contains a type not being types.Document")
		}
		if err != nil {
			return
		}
		fmt.Println("newest")
		fmt.Println(doc)
	}

	//for i := 0; i < len(*docs); i++ {
	//
	//	err = projectDocument(&(*docs)[i], projection, exclusion)
	//	if err != nil {
	//		return
	//	}
	//}

	return nil
}

func projectDocument(doc *types.Document, projection types.Document, exclusion bool) (err error) {

	projectionMap := projection.Map()
	fmt.Println("IGNOREKEYS")
	d, _ := (doc.Map())["ignoreKeys"]
	fmt.Println(d)
	fmt.Printf("%T\n", d)
	fmt.Println((doc.Map())["ignoreKeys"])
	for k1 := range doc.Map() {
		projectionVal, ok := projectionMap[k1]
		fmt.Println("k1")
		fmt.Println(k1)
		fmt.Println(projectionVal)
		fmt.Printf("%T\n", projectionVal)
		fmt.Println((doc.Map())[k1])
		fmt.Printf("%T\n", (doc.Map())[k1])

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
			//inclusion
			fmt.Println("1")
			doc.Remove(k1)
			continue
		}

		switch projectionVal := projectionVal.(type) { // found in the projection
		case bool: // field: bool
			fmt.Println("NOW HERE")
			if !projectionVal {
				fmt.Println("2")
				doc.Remove(k1)
				fmt.Println("new DOc")
				fmt.Println(doc)
			} else { // inclusion
				docVal, _ := (doc.Map())[k1]
				switch docVal.(type) {
				case nil:
					fmt.Println("NOW HERE 2")
					valType, _ := (doc.Map())["ignoreKeys"]
					switch keys := valType.(type) {
					case *types.Array:
						fmt.Println("made it")
						if !keys.Contains(k1) {
							fmt.Println("and again")
							doc.Remove(k1)
						}
					}

				}
			}

		//case int32, int64, float64: // field: number
		//	if compareScalars(projectionVal, int32(0)) == equal {
		//		doc.Remove(k1)
		//	}
		//
		//case *types.Document: // field: { $elemMatch: { field2: value }}
		//	if err := applyComplexProjection(k1, doc, projectionVal); err != nil {
		//		return err
		//	}

		default:
			return lazyerrors.Errorf("unsupported operation %s %v (%T)", k1, projectionVal, projectionVal)
		}
	}

	if !exclusion {
		doc.Remove("ignoreKeys")
	}
	return nil

}
