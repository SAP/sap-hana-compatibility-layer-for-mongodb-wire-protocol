// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"context"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
)

// MsgRolesinfo returns information about the given roles.
// Is a workaround to make it possible to connect and use GUI's like Studio 3T.
func (h *Handler) MsgRolesInfo(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	var reply wire.OpMsg
	msgDoc, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	db, err := msgDoc.Get("$db")
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	err = reply.SetSections(wire.OpMsgSection{
		Documents: []types.Document{types.MustMakeDocument(
			"roles", types.MustNewArray(
				types.MustMakeDocument(
					"role", "readWrite",
					"db", db.(string),
					"isBuiltIn", true,
					"roles", types.MustNewArray(),
					"inheretedRoles", types.MustNewArray(),
				),
			),
			"ok", float64(1),
		)},
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return &reply, nil
}
