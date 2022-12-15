// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"context"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
)

// MsgGetLastError is an implementation of the command getlasterror.
// Is a workaround to make it possible to connect and use GUI's like Studio 3T.
func (h *Handler) MsgGetLastError(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	var reply wire.OpMsg
	err := reply.SetSections(wire.OpMsgSection{
		Documents: []types.Document{types.MustMakeDocument(
			"errmsg", "This a custom error. Only used in GUI's as a workaround-",
			"code", int32(59),
			"codeName", "internalError",
			"ok", float64(1),
		)},
	})

	return &reply, err
}
