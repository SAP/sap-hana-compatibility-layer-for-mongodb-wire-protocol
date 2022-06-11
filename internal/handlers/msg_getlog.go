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
	"encoding/json"
	"time"

	"github.com/DocStore/HANA_HWY/internal/handlers/common"
	"github.com/DocStore/HANA_HWY/internal/types"
	"github.com/DocStore/HANA_HWY/internal/util/lazyerrors"
	"github.com/DocStore/HANA_HWY/internal/util/version"
	"github.com/DocStore/HANA_HWY/internal/wire"
)

// MsgGetLog is an administrative command that returns the most recent 1024 logged events.
func (h *Handler) MsgGetLog(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if l := document.Map()["getLog"]; l != "startupWarnings" {
		return nil, common.NewErrorMessage(common.ErrNotImplemented, "MsgGetLog: unhandled getLog value %q", l)
	}

	var hv string
	err = h.hanaPool.QueryRowContext(ctx, "Select VERSION from \"SYS\".\"M_DATABASE\";").Scan(&hv)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	// hv = strings.Split(hv, ".")[0]
	mv := version.Get()

	var log types.Array
	for _, line := range []string{
		"Powered by HANA HWY " + mv.Version + " and SAP HANA " + hv + ".",
	} {
		b, err := json.Marshal(map[string]any{
			"msg":  line,
			"tags": []string{"startupWarnings"},
			"s":    "I",
			"c":    "STORAGE",
			"id":   42000,
			"ctx":  "initandlisten",
			"t": map[string]string{
				"$date": time.Now().UTC().Format("2006-01-02T15:04:05.999Z07:00"),
			},
		})
		if err != nil {
			return nil, lazyerrors.Error(err)
		}
		if err = log.Append(string(b)); err != nil {
			return nil, lazyerrors.Error(err)
		}
	}

	var reply wire.OpMsg
	err = reply.SetSections(wire.OpMsgSection{
		Documents: []types.Document{types.MustMakeDocument(
			"totalLinesWritten", int32(log.Len()),
			"log", &log,
			"ok", float64(1),
		)},
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return &reply, nil
}
