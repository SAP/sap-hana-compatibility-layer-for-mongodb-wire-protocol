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
	"github.com/FerretDB/FerretDB/internal/pg"
	"github.com/FerretDB/FerretDB/internal/types"
)

func projection(projection types.Document, p *pg.Placeholder) (sql string, args []any, err error) {
	projectionMap := projection.Map()
	if len(projectionMap) == 0 {
		sql = "*"
		return
	}
	sql = "{\"keys\": '[\"_id\", \"fifth\"]'"
	sql += ", \"_id\": \"_id\""

	//for i, k := range projection.Keys() {
	//	if i != 0 {
	//		ks += ", "
	//	}
	//	ks += p.Next()
	//	args = append(args, k)
	//}

	for _, k := range projection.Keys() {
		sql += ", \"" + k + "\": \"" + k + "\""
		fmt.Println("sql so far")
		fmt.Println(sql)

	}

	//sql = "json_build_object('$k', array[" + ks + "],"
	//for i, k := range projection.Keys() {
	//	if i != 0 {
	//		sql += ", "
	//	}
	//	sql += p.Next() + "::text, _jsonb->" + p.Next()
	//	args = append(args, k, k)
	//}

	sql += "}"

	fmt.Println("finished projection SQL")
	fmt.Println(sql)

	return
}
