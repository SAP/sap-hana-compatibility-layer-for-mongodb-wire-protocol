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

package handlers

import (
	"context"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
)

// Is a workaround to make it possible to connect and use GUI's like Studio 3T.
func (h *Handler) MsgDBStats(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	m := document.Map()
	db := m["$db"].(string)
	scale, ok := m["scale"].(float64)
	if !ok {
		scale = 1
	}

	// TODO: Make a function DBStats for hanaPool getting
	// Needed information.
	// stats, err := h.hanaPool.DBStats(ctx, db)
	// if err != nil {
	// 	return nil, lazyerrors.Error(err)
	// }

	// Random values as hanaPool.DBStats not implemented.
	var reply wire.OpMsg
	err = reply.SetSections(wire.OpMsgSection{
		Documents: []types.Document{types.MustMakeDocument(
			"db", db,
			"collections", int32(1),
			"views", int32(0),
			"objects", int32(2),
			"avgObjSize", float64(12)/float64(2),
			"dataSize", float64(12)/scale,
			"indexes", int32(0),
			"indexSize", float64(0)/scale,
			"totalSize", float64(0)/scale,
			"scaleFactor", scale,
			"ok", float64(1),
		)},
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return &reply, nil
}
