package common

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.wdf.sap.corp/DocStore/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/fjson"
	"github.wdf.sap.corp/DocStore/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/hana"
	"github.wdf.sap.corp/DocStore/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/types"
)

func IsIdUnique(id any, db, collection string, ctx context.Context, hanapool *hana.Hpool) (unique bool, errMsg error, err error) {
	sql := "SELECT _id FROM %s.%s "

	whereSQL, errSQL := Where(types.MustMakeDocument([]any{"_id", id}...))
	if errSQL != nil {
		err = errSQL
		return
	}

	sql = fmt.Sprintf(sql, db, collection) + whereSQL + " LIMIT 1"

	var returnValue any
	ScanErr := hanapool.QueryRowContext(ctx, sql).Scan(&returnValue)

	if ScanErr != nil {
		if strings.EqualFold(ScanErr.Error(), "sql: no rows in result set") {
			unique = true

			return
		}
		err = ScanErr
		return
	}

	byteID, errMarshal := fjson.MarshalHANA(id)
	if errMarshal != nil {
		err = errMarshal
		return
	}

	msg := fmt.Sprintf("E11000 duplicate key error collection: %s.%s index: _id_ dup key: { _id: %s }", db, collection, string(byteID))
	if strings.Contains(msg, "{\"oid\":") {
		msg = strings.Replace(msg, "{\"oid\":", "", 1)
		msg = strings.Replace(msg, "}", "", 1)
	}
	errMsg = errors.New(msg)

	return
}
