package hana

import (
	_ "SAP/go-hdb/driver"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.wdf.sap.corp/DocStore/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
	"go.uber.org/zap"
)

var (
	ErrNotExist     = fmt.Errorf("schema or table does not exist")
	ErrAlreadyExist = fmt.Errorf("schema or table already exist")
)

type Hpool struct {
	*sql.DB
}

// TableStats describes some statistics for a table.
type TableStats struct {
	Table       string
	TableType   string
	SizeTotal   int32
	SizeIndexes int32
	SizeTable   int32
	Rows        int32
}

// DBStats describes some statistics for a database.
type DBStats struct {
	Name         string
	CountTables  int32
	CountRows    int32
	SizeTotal    int64
	SizeIndexes  int64
	SizeSchema   int64
	CountIndexes int32
}

func CreatePool(connString string, logger *zap.Logger, lazy bool) (*Hpool, error) {

	//Option 1, retrieve the connection parameters from the hdbuserstore
	//host, port, user name and password come from the hdbuserstore key USER1UserKey
	connectString := "please-insert-sap-hana-url-here"

	//Option 2, specify the connection parameters
	//connectString := "please-insert-sap-hana-url-here"

	//encrypt and sslValidateCertificate should be true for HANA Cloud connections
	//As of SAP HANA Client 2.6, connections on port 443 enable encryption by default

	fmt.Println("Connect String is " + connectString)

	db, err := sql.Open("hdb", connectString)
	if err != nil {
		return nil, fmt.Errorf("hanapool.CreatePool: %w", err)
	}

	res := &Hpool{
		DB: db,
	}

	return res, err

}

// Tables returns a sorted list of SAP HANA JSON Document Store collection names.
func (hanaPool *Hpool) Tables(ctx context.Context, db string) ([]string, error) {

	if err := hanaPool.CreateSchema(ctx, db); err != nil && err != ErrAlreadyExist {
		return nil, lazyerrors.Errorf("Handler.msgStorage: %w", err)
	}

	sql := "SELECT TABLE_NAME FROM \"PUBLIC\".\"M_TABLES\" WHERE SCHEMA_NAME = $1 AND TABLE_TYPE = 'COLLECTION';"
	rows, err := hanaPool.QueryContext(ctx, sql, strings.ToUpper(db))
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	defer rows.Close()

	res := make([]string, 0, 2)
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, lazyerrors.Error(err)
		}

		res = append(res, name)
	}
	if err = rows.Err(); err != nil {
		return nil, lazyerrors.Error(err)
	}

	return res, nil
}

// CreateSchema creates a schema in SAP HANA JSON Document Store.
func (hanaPool *Hpool) CreateSchema(ctx context.Context, db string) error {
	sql := `CREATE SCHEMA ` + db
	_, err := hanaPool.ExecContext(ctx, sql)

	if err != nil {
		return ErrAlreadyExist
	}

	return err
}

// CreateCollection creates a new SAP HANA JSON Document Store collection.
//
// It returns ErrAlreadyExist if collection already exist.
func (hanaPool *Hpool) CreateCollection(ctx context.Context, db, collection string) error {
	sql := `CREATE COLLECTION ` + db + "." + collection
	_, err := hanaPool.ExecContext(ctx, sql)

	if err != nil {
		return ErrAlreadyExist
	}

	return err
}

// Schemas returns a sorted list of SAP HANA JSON Document Store schema names.
func (hanaPool *Hpool) Schemas(ctx context.Context) ([]string, error) {
	sql := "SELECT * FROM SCHEMAS WHERE SCHEMA_NAME NOT LIKE '%SYS%' AND SCHEMA_OWNER NOT LIKE '%SYS%'"
	rows, err := hanaPool.QueryContext(ctx, sql)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	defer rows.Close()

	res := make([]string, 0, 2)
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, lazyerrors.Error(err)
		}

		if strings.HasPrefix(name, "pg_") || name == "information_schema" {
			continue
		}

		res = append(res, name)
	}
	if err = rows.Err(); err != nil {
		return nil, lazyerrors.Error(err)
	}

	return res, nil
}

// TableStats returns a set of statistics for a table.
// Still needs to be written for SAP HANA JSON Document Store
func (hanaPool *Hpool) TableStats(ctx context.Context, db, table string) (*TableStats, error) {
	res := new(TableStats)
	sql := `
    SELECT table_name, table_type,
           pg_total_relation_size('"'||t.table_schema||'"."'||t.table_name||'"'),
           pg_indexes_size('"'||t.table_schema||'"."'||t.table_name||'"'),
           pg_relation_size('"'||t.table_schema||'"."'||t.table_name||'"'),
           COALESCE(s.n_live_tup, 0)
      FROM information_schema.tables AS t
      LEFT OUTER
      JOIN pg_stat_user_tables AS s ON s.schemaname = t.table_schema
                                      and s.relname = t.table_name
     WHERE t.table_schema = $1
       AND t.table_name = $2`

	err := hanaPool.QueryRowContext(ctx, sql, db, table).
		Scan(&res.Table, &res.TableType, &res.SizeTotal, &res.SizeIndexes, &res.SizeTable, &res.Rows)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return res, nil
}

// DBStats returns a set of statistics for a database.
// Still needs to be written for SAP HANA JSON Document Store
func (hanaPool *Hpool) DBStats(ctx context.Context, db string) (*DBStats, error) {
	res := new(DBStats)
	sql := `
    SELECT COUNT(distinct t.table_name)                                                             AS CountTables,
           COALESCE(SUM(s.n_live_tup), 0)                                                           AS CountRows,
           COALESCE(SUM(pg_total_relation_size('"'||t.table_schema||'"."'||t.table_name||'"')), 0)  AS SizeTotal,
           COALESCE(SUM(pg_indexes_size('"'||t.table_schema||'"."'||t.table_name||'"')), 0)         AS SizeIndexes,
           COALESCE(SUM(pg_relation_size('"'||t.table_schema||'"."'||t.table_name||'"')), 0)        AS SizeSchema,
           COUNT(distinct i.indexname)                                                              AS CountIndexes
      FROM information_schema.tables AS t
      LEFT OUTER
      JOIN pg_stat_user_tables       AS s ON s.schemaname = t.table_schema
                                         AND s.relname = t.table_name
      LEFT OUTER
      JOIN pg_indexes                AS i ON i.schemaname = t.table_schema
                                         AND i.tablename = t.table_name
     WHERE t.table_schema = $1`

	res.Name = db
	err := hanaPool.QueryRowContext(ctx, sql, db).
		Scan(&res.CountTables, &res.CountRows, &res.SizeTotal, &res.SizeIndexes, &res.SizeSchema, &res.CountIndexes)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return res, nil
}

// DropTable drops collection
//
// It returns ErrNotExist is collection does not exist.
func (hanaPool *Hpool) DropTable(ctx context.Context, db, collection string) error {

	sql := `DROP COLLECTION ` + db + "." + collection
	_, err := hanaPool.ExecContext(ctx, sql)

	if err != nil {
		return ErrNotExist
	}

	return err
}

// DropSchema drops database
//
// It returns ErrNotExist if schema does not exist.
func (hanaPool *Hpool) DropSchema(ctx context.Context, db string) error {
	sql := `DROP SCHEMA ` + db + " cascade"
	_, err := hanaPool.ExecContext(ctx, sql)

	return err
}
