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
	"strings"
	"time"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/bson"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/handlers/common"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
)

func (h *Handler) QueryCmd(ctx context.Context, query *wire.OpQuery) (*wire.OpReply, error) {
	switch cmd := strings.ToLower(query.Query.Command()); cmd {
	case "ismaster":
		// TODO merge with MsgHello
		reply := &wire.OpReply{
			NumberReturned: 1,
			Documents: []types.Document{
				types.MustMakeDocument(
					"helloOk", true,
					"ismaster", true,
					// topologyVersion
					"maxBsonObjectSize", int32(bson.MaxDocumentLen),
					"maxMessageSizeBytes", int32(wire.MaxMsgLen),
					"maxWriteBatchSize", int32(100000),
					"localTime", time.Now(),
					// logicalSessionTimeoutMinutes
					// connectionId
					"minWireVersion", int32(13),
					"maxWireVersion", int32(13),
					"readOnly", false,
					"ok", float64(1),
				),
			},
		}
		return reply, nil
	case "getlasterror":
		reply := &wire.OpReply{
			NumberReturned: 1,
			Documents: []types.Document{
				types.MustMakeDocument(
					"errmsg", "This a custom error. Only used in GUI's as a workaround-",
					"code", int32(59),
					"codeName", "internalError",
					"ok", float64(1),
				),
			},
		}
		return reply, nil
	default:
		return nil, common.NewErrorMessage(common.ErrNotImplemented, "QueryCmd: unhandled command %q", cmd)
	}
}
