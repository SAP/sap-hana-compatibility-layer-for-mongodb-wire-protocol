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

package common

import (
	"fmt"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"go.uber.org/zap"
)

// Unimplemented returns ErrNotImplemented if doc has any of the given fields.
func Unimplemented(doc *types.Document, fields ...string) error {
	for _, field := range fields {
		if v, err := doc.Get(field); err == nil || v != nil {
			err = fmt.Errorf("%s: support for field %q is not implemented yet", doc.Command(), field)
			return NewError(ErrNotImplemented, err)
		}
	}

	return nil
}

// Ignored logs a message if doc has any of the given fields.
func Ignored(doc *types.Document, l *zap.Logger, fields ...string) {
	for _, field := range fields {
		if v, err := doc.Get(field); err == nil || v != nil {
			l.Debug("ignoring field", zap.String("command", doc.Command()), zap.String("field", field))
		}
	}
}
