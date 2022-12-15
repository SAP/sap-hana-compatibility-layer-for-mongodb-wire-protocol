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

import (
	"context"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/hana"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/handlers/common"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
)

// MsgCreate adds a collection and if the database is not created yet, it also creates a schema
func (h *Handler) MsgCreate(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	unimplementedFields := []string{
		"timeseries",
		"expireAfterSeconds",
		"size",
		"max",
		"validator",
		"validationLevel",
		"validationAction",
		"viewOn",
		"pipeline",
		"collation",
		"autoIndexId",
		"storageEngine",
		"indexOptionDefaults",
		"writeConcern",
		"comment",
	}
	if err := common.Unimplemented(&document, unimplementedFields...); err != nil {
		return nil, err
	}

	common.Ignored(&document, h.l, "capped")

	m := document.Map()
	if _, ok := m["viewOn"]; ok {
		return nil, common.NewErrorMessage(common.ErrCommandNotFound, "no such command: createView")
	}

	collection := m[document.Command()].(string)

	db := m["$db"].(string)
	if err := h.hanaPool.CreateSchema(ctx, db); err != nil && err != hana.ErrAlreadyExist {
		return nil, lazyerrors.Error(err)
	}

	if err = h.hanaPool.CreateCollection(ctx, db, collection); err != nil {
		if err == hana.ErrAlreadyExist {
			return nil, common.NewErrorMessage(common.ErrNamespaceExists, "Collection already exists. NS: %s.%s", db, collection)
		}
		return nil, lazyerrors.Error(err)
	}

	var reply wire.OpMsg
	err = reply.SetSections(wire.OpMsgSection{
		Documents: []types.Document{types.MustMakeDocument(
			"ok", float64(1),
		)},
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return &reply, nil
}
