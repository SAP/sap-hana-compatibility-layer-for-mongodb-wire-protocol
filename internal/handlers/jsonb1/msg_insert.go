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
	"context"
	"fmt"

	//"github.com/jackc/pgx/v4"

	"github.com/lucboj/FerretDB_SAP_HANA/internal/bson"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/types"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/util/lazyerrors"
	"github.com/lucboj/FerretDB_SAP_HANA/internal/wire"
)

// MsgInsert inserts a document or documents into a collection.
func (h *storage) MsgInsert(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	fmt.Println("Document Insert:")
	fmt.Println(document)

	m := document.Map()
	fmt.Println("m:")
	fmt.Println(m)

	collection := m[document.Command()].(string)
	fmt.Println("Collection:")
	fmt.Println(collection)

	//db := m["$db"].(string)
	docs, _ := m["documents"].(*types.Array)
	fmt.Println("docs:")
	fmt.Println(docs)
	var inserted int32
	for i := 0; i < docs.Len(); i++ {
		doc, err := docs.Get(i)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		d := doc.(types.Document)
		fmt.Println(d)
		sql := fmt.Sprintf("insert INTO %s VALUES ($1)", collection)
		t := bson.MustConvertDocument(d)
		fmt.Println("t")
		fmt.Println(t)
		b, err := bson.MustConvertDocument(d).MarshalJSONHANA()
		fmt.Println(sql)
		fmt.Println(b)
		if err != nil {
			return nil, err
		}

		if _, err = h.hanaPool.ExecContext(ctx, sql, b); err != nil {
			return nil, err
		}
		fmt.Println("hey")
		inserted++
	}

	var reply wire.OpMsg
	err = reply.SetSections(wire.OpMsgSection{
		Documents: []types.Document{types.MustMakeDocument(
			"n", inserted,
			"ok", float64(1),
		)},
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return &reply, nil
}
