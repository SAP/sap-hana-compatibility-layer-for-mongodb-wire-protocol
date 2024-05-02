// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"context"
	sqldb "database/sql"
	"fmt"
	"strings"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/bson"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/hana"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/handlers/common"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/wire"
)

type findAndModifyParams struct {
	db         string
	collection string
	filter     *types.Document
	update     *types.Document
	sort       *types.Document
	replace    bool
	remove     bool
	new        bool
	upsert     bool
	upsertDoc  *types.Document
	docID      any
}

// MsgFindAndModify finds documents in a collection or view and modifys or deletes them.
func (h *storage) MsgFindAndModify(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	unimplementedFields := []string{
		"arrayFilter",
		"commented",
		"let",
		"maxTimeMS",
	}
	if err := common.Unimplemented(&document, unimplementedFields...); err != nil {
		return nil, err
	}

	ignoredFields := []string{
		"fields",
		"bypassDocumentValidation",
		"writeConcern",
		"collation",
		"hint",
	}
	common.Ignored(&document, h.l, ignoredFields...)

	var params findAndModifyParams
	err = params.fillFindAndModifyParams(&document)
	if err != nil {
		return nil, err
	}

	doc, err := findDocument(ctx, &params, h.hanaPool)
	if err != nil {
		return nil, err
	}

	resp := &wire.OpMsg{}
	if doc != nil || params.upsert {
		err = modifyDocument(ctx, &params, h.hanaPool)
		if err != nil {
			return nil, err
		}

		if params.new && !params.remove {
			doc, err = findNewDocument(ctx, &params, h.hanaPool)
			if err != nil {
				return nil, err
			}
		}

		if params.remove {
			err = resp.SetSections(wire.OpMsgSection{
				Documents: []types.Document{types.MustMakeDocument(
					"lastErrorObject", types.MustMakeDocument(
						"n", int32(1),
					),
					"value", *doc,
					"ok", float64(1),
				)},
			})
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
		} else if doc == nil {
			// find better value for nil
			var value any
			if len(params.sort.Keys()) != 0 {
				value = types.MustMakeDocument()
			} else {
				value = nil
			}
			err = resp.SetSections(wire.OpMsgSection{
				Documents: []types.Document{types.MustMakeDocument(
					"lastErrorObject", types.MustMakeDocument(
						"n", int32(0),
						"updatedExisting", false,
					),
					"value", value,
					"ok", float64(1),
				)},
			})
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
		} else {
			err = resp.SetSections(wire.OpMsgSection{
				Documents: []types.Document{types.MustMakeDocument(
					"lastErrorObject", types.MustMakeDocument(
						"n", int32(1),
						"updatedExisting", true,
					),
					"value", *doc,
					"ok", float64(1),
				)},
			})
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
		}
	} else {
		if params.remove {
			err = resp.SetSections(wire.OpMsgSection{
				Documents: []types.Document{types.MustMakeDocument(
					"lastErrorObject", types.MustMakeDocument(
						"n", int32(0),
					),
					"ok", float64(1),
				)},
			})
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
		} else {
			// find better value for nil
			err = resp.SetSections(wire.OpMsgSection{
				Documents: []types.Document{types.MustMakeDocument(
					"lastErrorObject", types.MustMakeDocument(
						"n", int32(0),
						"updatedExisting", false,
					),
					"value", nil,
					"ok", float64(1),
				)},
			})
			if err != nil {
				return nil, lazyerrors.Error(err)
			}
		}
	}

	return resp, nil
}

func findDocument(ctx context.Context, params *findAndModifyParams, db *hana.Hpool) (*types.Document, error) {
	if exists, err := db.NamespaceExists(ctx, params.db, params.collection); err == nil {
		if !exists {
			if params.upsert {
				err = db.CreateNamespaceIfNotExists(ctx, params.db, params.collection)
				return nil, err
			}
			return nil, nil
		}
	} else {
		return nil, err
	}

	sql, err := createQuery(ctx, params)
	if err != nil {
		return nil, err
	}

	var docByte []byte
	row := db.QueryRowContext(ctx, sql)

	err = row.Scan(&docByte)
	if err != nil {
		if err == sqldb.ErrNoRows {
			return nil, nil
		}
		return nil, lazyerrors.Error(err)
	}

	var doc bson.Document
	if err := doc.UnmarshalJSON(docByte); err != nil {
		return nil, lazyerrors.Error(err)
	}

	d := types.MustConvertDocument(&doc)

	params.docID, err = d.Get("_id")
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return &d, nil

}

func modifyDocument(ctx context.Context, params *findAndModifyParams, db *hana.Hpool) error {
	var err error

	if params.docID == nil {
		params.upsertDoc, err = common.Upsert(params.update, params.filter, params.replace)
		if err != nil {
			return lazyerrors.Error(err)
		}
		err = upsertDocument(ctx, params, db)
	} else if params.remove {
		err = removeDocument(ctx, params, db)
	} else if params.replace {
		err = replaceDocument(ctx, params, db)
	} else if params.update != nil {
		err = updateDocument(ctx, params, db)
	} else {
		return common.NewErrorMessage(common.ErrBadValue, "Usage of findAndModify seems incorrect")
	}

	return err
}

func findNewDocument(ctx context.Context, params *findAndModifyParams, db *hana.Hpool) (*types.Document, error) {
	sql := fmt.Sprintf("SELECT * FROM \"%s\".\"%s\"", params.db, params.collection)

	whereSQL, err := common.CreateWhereClause(types.MustMakeDocument("_id", params.docID))
	if err != nil {
		return nil, err
	}

	sql += whereSQL + " LIMIT 1"

	row := db.QueryRowContext(ctx, sql)

	var docByte []byte
	err = row.Scan(&docByte)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	var doc bson.Document
	if err := doc.UnmarshalJSON(docByte); err != nil {
		return nil, lazyerrors.Error(err)
	}

	d := types.MustConvertDocument(&doc)

	return &d, nil
}

func createQuery(ctx context.Context, params *findAndModifyParams) (string, error) {
	sql := fmt.Sprintf("SELECT * FROM \"%s\".\"%s\"", params.db, params.collection)

	whereSQL, err := common.CreateWhereClause(*params.filter)
	if err != nil {
		return "", lazyerrors.Error(err)
	}

	orderSQL, err := createOrderBy(params)
	if err != nil {
		return "", lazyerrors.Error(err)
	}

	sql += whereSQL + orderSQL

	sql += " LIMIT 1"

	return sql, nil
}

func createOrderBy(params *findAndModifyParams) (sql string, err error) {
	if params.sort == nil {
		return "", nil
	}
	sortMap := params.sort.Map()
	if len(sortMap) != 0 {
		sql += " ORDER BY "

		for i, sortKey := range params.sort.Keys() {
			if i != 0 {
				sql += ","
			}

			if strings.Contains(sortKey, ".") {
				split := strings.Split(sortKey, ".")
				sql += " "
				for j, s := range split {
					if (len(split) - 1) == j {
						sql += "\"" + s + "\""
					} else {
						sql += "\"" + s + "\"."
					}
				}
			} else {
				sql += "\"" + sortKey + "\" "
			}

			order, ok := sortMap[sortKey].(int32)
			if !ok {
				if !anyIsInt(sortMap[sortKey]) {
					err = common.NewErrorMessage(common.ErrSortBadValue, "cannot use type %T for sort", sortMap[sortKey])
					return
				}
				order = int32(sortMap[sortKey].(float64))
			}
			if order == 1 {
				sql += " ASC"
			} else if order == -1 {
				sql += " DESC"
			} else {
				err = common.NewErrorMessage(common.ErrSortBadValue, "cannot use value %s for sort", sortMap[sortKey])
			}
		}
	}
	return
}

func removeDocument(ctx context.Context, params *findAndModifyParams, db *hana.Hpool) error {
	sql := fmt.Sprintf("DELETE FROM \"%s\".\"%s\"", params.db, params.collection)

	whereSQL, err := common.CreateWhereClause(types.MustMakeDocument("_id", params.docID))
	if err != nil {
		return lazyerrors.Error(err)
	}

	sql += whereSQL

	_, err = db.ExecContext(ctx, sql)

	return err
}

func updateDocument(ctx context.Context, params *findAndModifyParams, db *hana.Hpool) error {

	sql := fmt.Sprintf("UPDATE \"%s\".\"%s\"", params.db, params.collection)

	whereSQL, err := common.CreateWhereClause(types.MustMakeDocument("_id", params.docID))
	if err != nil {
		return lazyerrors.Error(err)
	}

	updateSQL, _, err := common.Update(*params.update)
	if err != nil {
		return lazyerrors.Error(err)
	}

	sql += updateSQL + whereSQL

	_, err = db.ExecContext(ctx, sql)

	return err
}

func replaceDocument(ctx context.Context, params *findAndModifyParams, db *hana.Hpool) error {

	err := removeDocument(ctx, params, db)
	if err != nil {
		return err
	}
	err = insertDocument(ctx, params, db)

	return err
}

func insertDocument(ctx context.Context, params *findAndModifyParams, db *hana.Hpool) error {

	var err error
	var id any

	if id, err = params.update.Get("_id"); err == nil {
		uniqueId, errMsg, err := common.IsIdUnique(id, params.db, params.collection, ctx, db)
		if err != nil {
			return lazyerrors.Error(err)
		}
		if !uniqueId {
			return errMsg
		}
	}

	doc := params.update

	if id != nil {
		params.docID = id
		doc.Set("_id", id)
	} else {
		doc.Set("_id", params.docID)
	}

	sql := fmt.Sprintf("INSERT INTO \"%s\".\"%s\" VALUES ($1)", params.db, params.collection)

	b, err := bson.MustConvertDocument(doc).MarshalJSONHANA()
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, sql, b)

	return err
}

func upsertDocument(ctx context.Context, params *findAndModifyParams, db *hana.Hpool) error {

	var err error
	var id any

	if id, err = params.upsertDoc.Get("_id"); err == nil {
		uniqueId, errMsg, err := common.IsIdUnique(id, params.db, params.collection, ctx, db)
		if err != nil {
			return lazyerrors.Error(err)
		}
		if !uniqueId {
			return errMsg
		}
	} else {
		return fmt.Errorf("upsert document contains no object id")
	}
	if params.new {
		params.docID = id
	}

	sql := fmt.Sprintf("INSERT INTO \"%s\".\"%s\" VALUES ($1)", params.db, params.collection)

	b, err := bson.MustConvertDocument(params.upsertDoc).MarshalJSONHANA()
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, sql, b)

	return err
}

func (params *findAndModifyParams) fillFindAndModifyParams(doc *types.Document) error {
	var ok bool
	docMap := doc.Map()

	params.db, ok = docMap["$db"].(string)
	if !ok {
		return fmt.Errorf("key $db not found in document")
	}

	command := doc.Command()
	params.collection, ok = docMap[command].(string)
	if !ok {
		return fmt.Errorf("key %s not found in document", command)
	}

	filter, ok := docMap["query"].(types.Document)
	if !ok {
		return fmt.Errorf("key \"query\" not found in document")
	}

	params.filter = &filter

	var updateSet bool
	if update, ok := docMap["update"]; ok {
		if updateDoc, ok := update.(types.Document); ok {
			params.update = &updateDoc

			var err error
			params.replace, err = checkIfReplace(params.update)
			if err != nil {
				return err
			}
			updateSet = true
		} else {
			return common.NewErrorMessage(common.ErrBadValue, "argument \"update\" must be an object")
		}
	}

	if remove, ok := docMap["remove"]; ok {
		if removeVal, ok := remove.(bool); ok {
			params.remove = removeVal
		} else {
			return common.NewErrorMessage(common.ErrBadValue, "argument \"remove\" only supported as boolean")
		}
	}

	if updateSet && params.remove {
		return lazyerrors.Errorf("argument \"update\" cannot be specified when \"remove\" is true")
	}

	var sortDoc types.Document
	sort, ok := docMap["sort"]
	if ok {
		sortDoc, ok = sort.(types.Document)
		if !ok {
			return common.NewErrorMessage(common.ErrBadValue, "expected sort to be document but got %s as %T", sort, sort)
		}
	}
	params.sort = &sortDoc

	params.new, _ = docMap["new"].(bool)

	params.upsert, _ = docMap["upsert"].(bool)

	fields, ok := docMap["fields"].(types.Document)
	if ok {
		if len(fields.Keys()) != 0 {
			return common.NewErrorMessage(common.ErrNotImplemented, "argument \"fields\" is not implemented yet")
		}
	}

	return nil
}

func checkIfReplace(doc *types.Document) (bool, error) {
	supportedUpdateCmds := map[string]struct{}{"$set": {}, "$unset": {}}

	for k := range doc.Map() {
		if strings.HasPrefix(k, "$") {
			if _, ok := supportedUpdateCmds[strings.ToLower(k)]; !ok {
				return false, fmt.Errorf("%s is not supported in update document", k)
			}

			return false, nil
		}
	}

	return true, nil
}
