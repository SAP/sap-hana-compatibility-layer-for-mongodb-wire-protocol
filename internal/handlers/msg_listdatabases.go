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

// MsgListDatabases command provides a list of all existing databases along with basic statistics about them.
func (h *Handler) MsgListDatabases(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	databaseNames, err := h.hanaPool.Schemas(ctx)
	if err != nil {
		return nil, err
	}
	databases := types.MakeArray(len(databaseNames))
	for _, databaseName := range databaseNames {
		tables, err := h.hanaPool.Tables(ctx, databaseName)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		var sizeOnDisk int64
		for _, name := range tables {
			var tableSize any
			err = h.hanaPool.QueryRowContext(ctx, "SELECT TABLE_SIZE FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = $1 AND TABLE_NAME = $2 AND TABLE_TYPE = 'COLLECTION';", databaseName, name).Scan(&tableSize)
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
			switch tableSize := tableSize.(type) {
			case int64: // collection is in memory and a size can be calculated
				sizeOnDisk += tableSize
			case nil: // collection is not in memory and no size can be calculated
				continue
			default:
				return nil, lazyerrors.Errorf("Got wrong type for tableSize. Got: %T", tableSize)
			}

		}

		d := types.MustMakeDocument(
			"name", databaseName,
			"sizeOnDisk", sizeOnDisk,
			"empty", sizeOnDisk == 0,
		)
		if err = databases.Append(d); err != nil {
			return nil, lazyerrors.Error(err)
		}
	}

	totalSize := int64(30)
	var reply wire.OpMsg
	err = reply.SetSections(wire.OpMsgSection{
		Documents: []types.Document{types.MustMakeDocument(
			"databases", databases,
			"totalSize", totalSize,
			"totalSizeMb", totalSize/1024/1024,
			"ok", float64(1),
		)},
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return &reply, nil
}
