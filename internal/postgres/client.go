package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

type (
	Schema struct {
		Name        string `db:"schema_name"`
		Owner       string `db:"schema_owner"`
		CatalogName string `db:"catalog_name"`
		TableCount  int    `db:"table_count"`
	}

	PGTable struct {
		Catalog     string `db:"table_catalog"`
		Name        string `db:"table_name"`
		Owner       string `db:"table_type"`
		Schema      string `db:"table_schema"`
		TableSize   int    `db:"table_size"`
		IndexesSize int    `db:"indexes_size"`
		TotalSize   int    `db:"total_size"`
	}

	Column struct {
		Catalog  string `db:"table_catalog"`
		Schema   string `db:"table_schema"`
		Name     string `db:"table_name"`
		Column   string `db:"column_name"`
		Nullable string `db:"is_nullable"`
		Type     string `db:"data_type"`
	}

	View struct {
		Name                  string `db:"view_name"`
		Definition            string `db:"definition"`
		IsPopulated           bool   `db:"ispopulated"`
		Owner                 string `db:"owner"`
		ViewSchema            string `db:"schema_name"`
		ReferencedTableSchema string `db:"referenced_table_schema"`
		ReferencedTableName   string `db:"referenced_table_name"`
	}
)

type Client struct {
	db *sqlx.DB
}

func New(db *sqlx.DB) *Client {
	return &Client{db: db}
}

func (c *Client) Schemas(ctx context.Context) ([]Schema, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		WITH table_counts AS (
			SELECT table_schema, count(*) as table_count
			FROM information_schema.tables
			WHERE table_type = 'BASE TABLE'
			GROUP BY table_schema
			ORDER BY table_schema
		)
		SELECT s.schema_name, s.schema_owner, s.catalog_name, COALESCE(tc.table_count, 0) as table_count
		FROM information_schema.schemata s
		FULL JOIN table_counts tc ON s.schema_name = tc.table_schema
		ORDER BY schema_owner, schema_name`

	var schemas []Schema
	return schemas, c.db.SelectContext(ctx, &schemas, query)
}

func (c *Client) SchemasUserCreated(ctx context.Context) ([]Schema, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		WITH table_counts AS (
			SELECT table_schema, count(*) as table_count
			FROM information_schema.tables
			WHERE table_type = 'BASE TABLE'
			GROUP BY table_schema
			ORDER BY table_schema
		),
		sch AS (
			SELECT schema_name, catalog_name FROM information_schema.schemata
		)
		SELECT s.nspname as schema_name, u.usename as schema_owner, COALESCE(tc.table_count, 0) as table_count, sch.catalog_name
		FROM pg_catalog.pg_namespace s
		JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
		FULL JOIN sch ON sch.schema_name = s.nspname
		FULL JOIN table_counts tc ON s.nspname = tc.table_schema
		WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'public')
			AND nspname NOT LIKE 'pg_toast%'
    	  	AND nspname NOT LIKE 'pg_temp_%'
	`
	var schemas []Schema
	return schemas, c.db.SelectContext(ctx, &schemas, query)
}

func (c *Client) Tables(ctx context.Context) ([]PGTable, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		WITH table_schemas AS (
			SELECT table_schema, table_name, ('"' || table_schema || '"."' || table_name || '"') AS table_name_alias
			FROM information_schema.tables
		),
		sizes AS (
			SELECT
		    	table_schema, table_name,
				pg_table_size(table_name_alias) AS table_size,
				pg_indexes_size(table_name_alias) AS indexes_size,
				pg_total_relation_size(table_name_alias) AS total_size
			FROM table_schemas
		)
		SELECT it.table_schema, it.table_name, it.table_catalog, it.table_type,
				sz.table_size, sz.indexes_size, sz.total_size
		FROM information_schema.tables it
		FULL JOIN sizes sz USING(table_name)
		ORDER BY table_schema, table_name`

	var tables []PGTable
	return tables, c.db.SelectContext(ctx, &tables, query)
}

func (c *Client) DescribeTable(ctx context.Context, table string) ([]Column, error) {
	if table == "" {
		return nil, errors.New("no table provided")
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT table_catalog, table_schema, table_name, column_name, is_nullable, data_type
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE table_name = $1`

	var cols []Column
	err := c.db.SelectContext(ctx, &cols, query, table)
	if err != nil {
		return nil, err
	}
	return cols, nil
}

func (c *Client) Views(ctx context.Context) ([]View, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT u.view_schema as schema_name, u.view_name, u.table_schema as referenced_table_schema,
       			u.table_name as referenced_table_name, v.view_definition as definition
		FROM information_schema.view_table_usage u
		JOIN information_schema.views v ON u.view_schema = v.table_schema AND u.view_name = v.table_name
		WHERE u.table_schema NOT IN ('information_schema', 'pg_catalog')
		ORDER BY u.view_schema, u.view_name`

	var views []View
	return views, c.db.SelectContext(ctx, &views, query)
}

func (c *Client) MaterializedViews(ctx context.Context) ([]View, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT schemaname as schema_name, matviewname as view_name, matviewowner as owner, ispopulated, definition
		FROM pg_matviews
		ORDER BY schema_name, view_name`

	var views []View
	return views, c.db.SelectContext(ctx, &views, query)
}

func (c *Client) TablesBySize(ctx context.Context) ([]PGTable, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT schemaname as table_schema,
				relname as table_name,
				pg_size_pretty(pg_relation_size(relid)) as table_data_size
		FROM pg_catalog.pg_statio_user_tables
		ORDER BY pg_relation_size(relid) desc`

	return readTables(ctx, c.db, query)
}

func (c *Client) TablesBySizeWithIndex(ctx context.Context) ([]PGTable, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	query := `
		SELECT schemaname as table_schema, relname as table_name,
				pg_size_pretty(pg_total_relation_size(relid)) as table_data_size,
				pg_size_pretty(pg_relation_size(relid)) as data_size,
				pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as external_size
		FROM pg_catalog.pg_statio_user_tables
		ORDER BY pg_total_relation_size(relid) desc, pg_relation_size(relid) desc`

	return readTables(ctx, c.db, query)
}

func readTables(ctx context.Context, db *sqlx.DB, query string) ([]PGTable, error) {
	var tables []PGTable
	err := db.SelectContext(ctx, &tables, query)
	if err != nil {
		return nil, err
	}
	return tables, nil
}
