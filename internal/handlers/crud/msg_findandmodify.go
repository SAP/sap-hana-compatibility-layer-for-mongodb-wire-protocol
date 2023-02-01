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
	remove     bool
	new        bool
	docID      any
}

func (h *storage) MsgFindAndModify(ctx context.Context, msg *wire.OpMsg) (*wire.OpMsg, error) {
	document, err := msg.Document()
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	unimplementedFields := []string{
		"upsert",
		"arrayFilter",
		"commented",
		"fields",
		"let",
		"maxTimeMS",
	}
	if err := common.Unimplemented(&document, unimplementedFields...); err != nil {
		return nil, err
	}

	ignoredFields := []string{
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
	if doc != nil {
		err = modifyDocument(ctx, &params, h.hanaPool)
		if err != nil {
			return nil, err
		}
		if params.new {
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
					"value", doc,
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
					"value", doc,
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
			err = resp.SetSections(wire.OpMsgSection{
				Documents: []types.Document{types.MustMakeDocument(
					"lastErrorObject", types.MustMakeDocument(
						"n", int32(0),
						"updatedExisting", false,
					),
					"value", types.Null,
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

	sql, err := createQuery(ctx, params)
	if err != nil {
		return nil, lazyerrors.Error(err)
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
	var b []byte
	if err := doc.UnmarshalJSON(b); err != nil {
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
	var sql string
	if params.remove {
		sql, err = createRemoveSQL(params)
	} else {
		sql, err = createUpdateSQL(params)
	}
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, sql)
	if err != nil {
		return lazyerrors.Error(err)
	}

	return nil
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
	var b []byte
	if err := doc.UnmarshalJSON(b); err != nil {
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

func createRemoveSQL(params *findAndModifyParams) (string, error) {

	sql := fmt.Sprintf("DELETE FROM \"%s\".\"%s\"")

	whereSQL, err := common.CreateWhereClause(types.MustMakeDocument("_id", params.docID))
	if err != nil {
		return "", lazyerrors.Error(err)
	}

	sql += whereSQL

	return sql, nil
}

func createUpdateSQL(params *findAndModifyParams) (string, error) {

	sql := fmt.Sprintf("UPDATE \"%s\".\"%s\"", params.db, params.collection)

	whereSQL, err := common.CreateWhereClause(types.MustMakeDocument("_id", params.docID))
	if err != nil {
		return "", lazyerrors.Error(err)
	}

	sql += whereSQL

	return sql, nil
}

func (params *findAndModifyParams) fillFindAndModifyParams(doc *types.Document) error {
	var ok bool
	docMap := doc.Map()

	command := doc.Command()
	params.db, ok = docMap["$db"].(string)
	if !ok {
		return fmt.Errorf("key $db not found in document")
	}
	params.collection, ok = docMap[command].(string)
	if !ok {
		return fmt.Errorf("key %s not found in document", command)
	}
	params.filter, ok = docMap["query"].(*types.Document)
	if !ok {
		return fmt.Errorf("key \"query\" not found in document")
	}
	params.update, ok = docMap["update"].(*types.Document)
	if !ok {
		return fmt.Errorf("key \"update\" not found in document")
	}
	params.sort, ok = docMap["sort"].(*types.Document)
	if !ok {
		return fmt.Errorf("key \"sort\" not found in document")
	}
	params.remove, ok = docMap["remove"].(bool)
	if !ok {
		return fmt.Errorf("key \"remove\" not found in document")
	}
	params.new, ok = docMap["new"].(bool)
	if !ok {
		return fmt.Errorf("key \"new\" not found in document")
	}

	return nil
}
