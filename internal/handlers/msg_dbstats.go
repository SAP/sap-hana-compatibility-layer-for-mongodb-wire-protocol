// SPDX-FileCopyrightText: 2021 FerretDB Inc.
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

package handlers

// import (
// 	"context"

// 	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
// 	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
// 	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
// )

// func (h *Handler) MsgDBStats(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
// 	document, err := msg.Document()
// 	if err != nil {
// 		return nil, lazyerrors.Error(err)
// 	}

// 	m := document.Map()
// 	db := m["$db"].(string)
// 	scale, ok := m["scale"].(float64)
// 	if !ok {
// 		scale = 1
// 	}

// 	stats, err := h.hanaPool.DBStats(ctx, db)
// 	if err != nil {
// 		return nil, lazyerrors.Error(err)
// 	}

// 	var reply wire.OpMsg
// 	err = reply.SetSections(wire.OpMsgSection{
// 		Documents: []types.Document{types.MustMakeDocument(
// 			"db", db,
// 			"collections", stats.CountTables,
// 			"views", int32(0),
// 			"objects", stats.CountRows,
// 			"avgObjSize", float64(stats.SizeSchema)/float64(stats.CountRows),
// 			"dataSize", float64(stats.SizeSchema)/scale,
// 			"indexes", stats.CountIndexes,
// 			"indexSize", float64(stats.SizeIndexes)/scale,
// 			"totalSize", float64(stats.SizeTotal)/scale,
// 			"scaleFactor", scale,
// 			"ok", float64(1),
// 		)},
// 	})
// 	if err != nil {
// 		return nil, lazyerrors.Error(err)
// 	}

// 	return &reply, nil
// }
