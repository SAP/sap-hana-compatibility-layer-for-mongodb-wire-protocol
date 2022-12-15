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

// MsgUsersInfo returns information about users.
// Is a workaround to make it possible to connect and use GUI's like Studio 3T.
func (h *Handler) MsgUsersInfo(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
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
			"users", types.MustNewArray(
				types.MustMakeDocument(
					"_id", db.(string)+".USERNAME",
					"user_id", "ffaac8a8-50f2-423b-b960-e1555a769372",
					"user", "USERNAME",
					"db", db.(string),
					"mechanisms", types.MustNewArray(),
					"customData", types.MustMakeDocument(),
					"roles", types.MustNewArray(),
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
