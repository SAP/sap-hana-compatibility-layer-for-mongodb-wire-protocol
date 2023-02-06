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
	"testing"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/testutil"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
	"github.com/stretchr/testify/assert"
)

func TestCommands(t *testing.T) {
	t.Run("Command name matches key", func(t *testing.T) {
		t.Parallel()
		for key, command := range commands {
			assert.Equal(t, key, command.name)
		}
	})
}

func TestSupportedCommands(t *testing.T) {
	var reply *wire.OpMsg

	ctx := testutil.Ctx(t)

	supportedCommands, err := SupportedCommands(ctx, reply)

	assert.Nil(t, err)

	expectedCommands := types.MustMakeDocument(
		"commands", types.MustMakeDocument(
			"debug_panic", types.MustMakeDocument(
				"help", "Used for debugging purposes.",
			),
			"drop", types.MustMakeDocument(
				"help", "Drops the collection.",
			),
			"getLog", types.MustMakeDocument(
				"help", "Returns the most recent logged events from memory.",
			),
			"create", types.MustMakeDocument(
				"help", "Creates the collection.",
			),
			"hostInfo", types.MustMakeDocument(
				"help", "Returns a summary of the system information.",
			),
			"hello", types.MustMakeDocument(
				"help", "Returns the role of the SAP HANA compatibility layer for MongoDB Wire Protocol instance.",
			),
			"listCollections", types.MustMakeDocument(
				"help", "Returns the information of the collections and views in the database.",
			),
			"ping", types.MustMakeDocument(
				"help", "Returns a pong response. Used for testing purposes.",
			),
			"buildInfo", types.MustMakeDocument(
				"help", "Returns a summary of the build information.",
			),
			"authenticate", types.MustMakeDocument(
				"help", "a method for authentication",
			),
			"debug_error", types.MustMakeDocument(
				"help", "Used for debugging purposes.",
			),
			"listCommands", types.MustMakeDocument(
				"help", "Returns information about the currently supported commands.",
			),
			"dropDatabase", types.MustMakeDocument(
				"help", "Deletes the database.",
			),
			"isMaster", types.MustMakeDocument(
				"help", "Returns the role of the SAP HANA compatibility layer for MongoDB Wire Protocol instance.",
			),
			"whatsmyuri", types.MustMakeDocument(
				"help", "An internal command.",
			),
			"find", types.MustMakeDocument(
				"help", "Returns documents matched by the custom query.",
			),
			"findAndModify", types.MustMakeDocument(
				"help", "find one document, modifies it and return either the old document or the new document.",
			),
			"count", types.MustMakeDocument(
				"help", "Returns the count of documents that's matched by the query.",
			),
			"delete", types.MustMakeDocument(
				"help", "Deletes documents matched by the query.",
			),
			"insert", types.MustMakeDocument(
				"help", "Inserts documents into the database.",
			),
			"update", types.MustMakeDocument(
				"help", "Updates documents that are matched by the query.",
			),
			"listDatabases", types.MustMakeDocument(
				"help", "Returns a summary of all the databases.",
			),
			"getlasterror", types.MustMakeDocument(
				"help", "Does not return last error. Is used as a workaround to allow use of some GUIs.",
			),
			"getLastError", types.MustMakeDocument(
				"help", "Does not return last error. Is used as a workaround to allow use of some GUIs.",
			),
			"usersInfo", types.MustMakeDocument(
				"help", "Returns user USERNAME. Is used as a workaround to allow use of some GUIs",
			),
			"rolesInfo", types.MustMakeDocument(
				"help", "Return role readWrite. Is used as a workaround to allow use of some GUIs",
			),
			"connectionStatus", types.MustMakeDocument(
				"help", "checks connection",
			),
			"dbStats", types.MustMakeDocument(
				"help", "Returns the statistics of the database.",
			),
		),
	)
	actualCommands, err := supportedCommands.Document()
	expected, _ := expectedCommands.Get("commands")
	actual, _ := actualCommands.Get("commands")
	assert.Nil(t, err)
	assert.Equal(t, expected.(types.Document).Map(), actual.(types.Document).Map())
}
