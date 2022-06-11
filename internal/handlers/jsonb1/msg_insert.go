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

	"github.com/DocStore/HANA_HWY/internal/bson"
	"github.com/DocStore/HANA_HWY/internal/handlers/common"
	"github.com/DocStore/HANA_HWY/internal/types"
	"github.com/DocStore/HANA_HWY/internal/util/lazyerrors"
	"github.com/DocStore/HANA_HWY/internal/wire"
)

// MsgInsert inserts a document or documents into a collection.
func (h *storage) MsgInsert(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	err = (common.Unimplemented(&document, "writeConcern", "bypassDocumentValidation", "comment"))
	if err != nil {
		return nil, err
	}

	common.Ignored(&document, h.l, "ordered")

	m := document.Map()

	collection := m[document.Command()].(string)

	docs, _ := m["documents"].(*types.Array)

	var inserted int32
	for i := 0; i < docs.Len(); i++ {
		doc, err := docs.Get(i)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		d := doc.(types.Document)

		sql := fmt.Sprintf("INSERT INTO %s VALUES ($1)", collection)

		b, err := bson.MustConvertDocument(d).MarshalJSONHANA()
		if err != nil {
			return nil, err
		}

		if _, err = h.hanaPool.ExecContext(ctx, sql, b); err != nil {
			return nil, err
		}
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
