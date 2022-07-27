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
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/hana"

	"go.uber.org/zap"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/handlers/common"
)

type storage struct {
	hanaPool *hana.Hpool
	l        *zap.Logger
}

func NewStorage(hanaPool *hana.Hpool, l *zap.Logger) common.Storage {
	return &storage{
		hanaPool: hanaPool,
		l:        l,
	}
}
