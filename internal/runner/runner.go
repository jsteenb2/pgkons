package runner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jsteenb2/pgkons/internal/postgres"

	"github.com/jmoiron/sqlx"
	"github.com/jsteenb2/promptui"
	"github.com/jsteenb2/promptui/list"
	"golang.org/x/sys/unix"
)

func validateEmptyInput(label string) func(input string) error {
	return func(input string) error {
		if input == "" {
			return errors.New("must provide a " + label)
		}
		return nil
	}
}

type (
	Schema struct {
		Name        string `db:"schema_name"`
		Owner       string `db:"schema_owner"`
		CatalogName string `db:"catalog_name"`
	}

	PGTable struct {
		Catalog string `db:"table_catalog"`
		Name    string `db:"table_name"`
		Owner   string `db:"table_type"`
		Schema  string `db:"table_schema"`
		Size    string `db:"table_data_size"`
	}
)

type Runner struct {
	db       *sqlx.DB
	pgClient *postgres.Client
}

func New(db *sql.DB) *Runner {
	dbx := sqlx.NewDb(db, "postgres")
	return &Runner{
		db:       dbx,
		pgClient: postgres.New(dbx),
	}
}

func (r *Runner) Run(ctx context.Context, debug bool) error {
	var err error
	for fn := startState.Fn; fn != nil; {
		fn, err = fn(ctx, r)
		if fn == nil && err == nil {
			fn = startState.Fn
		}
	}
	return err
}

func (r *Runner) Close() error {
	return r.db.Close()
}

var (
	schemaTemplates = func() *promptui.SelectTemplates {
		return &promptui.SelectTemplates{
			Label:    "{{ . }}",
			Active:   "» {{ .Name | bold|  cyan }} ({{ .Owner | bold | red }})",
			Inactive: "   {{ .Name | cyan }} ({{ .Owner | red }})",
			Details: `
 --------- Schema ----------
 {{ "Name:" | faint }}	{{ .Name }}
 {{ "Owner:" | faint }}	{{ .Owner }}
 {{ "Catalog name:" | faint }}	{{ .CatalogName }}
 {{ "Tables in Schema:" | faint }}	{{ .TableCount }}`,
		}
	}

	schemaSearcher = func(schemas []postgres.Schema) func(string, int) bool {
		return func(input string, index int) bool {
			sch := schemas[index]
			name := strings.Replace(strings.ToLower(sch.Name), " ", "", -1)
			input = strings.Replace(strings.ToLower(input), " ", "", -1)
			return strings.Contains(name, input)
		}
	}
)

func (r *Runner) Schemas(ctx context.Context) error {
	schemas, err := r.pgClient.Schemas(ctx)
	if err != nil {
		return err
	}
	return selecter("Schemas", schemas, schemaSearcher(schemas), schemaTemplates())
}

func (r *Runner) SchemasUserCreated(ctx context.Context) error {
	schemas, err := r.pgClient.SchemasUserCreated(ctx)
	if err != nil {
		return err
	}

	if len(schemas) == 0 {
		schemas = append(schemas, postgres.Schema{Name: "back"})
	}

	return selecter("Schemas", schemas, schemaSearcher(schemas), schemaTemplates())
}

func (r *Runner) Tables(ctx context.Context) error {
	tables, err := r.pgClient.Tables(ctx)
	if err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Schema | bold | green }}.{{ .Name | bold | cyan }}",
		Inactive: "  {{ .Schema | green }}.{{ .Name | cyan }}",
		Details: `
 --------- Table ----------
 {{ "Name:" | faint }}	{{ .Name }}
 {{ "Table Size:" | faint }}	{{ .TableSize }}
 {{ "Index Size:" | faint }}	{{ .IndexesSize }}
 {{ "Total Size:" | faint }}	{{ .TotalSize }}`,
	}

	searcher := func(input string, index int) bool {
		table := tables[index]
		name := strings.Replace(strings.ToLower(table.Name), " ", "", -1)
		schema := strings.Replace(strings.ToLower(table.Schema), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		combined := schema + "." + name
		return strings.Contains(combined, input)
	}

	return selecter("Tables", tables, searcher, templates)
}

func (r *Runner) DescribeTable(ctx context.Context, table string) error {
	_, err := r.pgClient.DescribeTable(ctx, table)
	return err
}

func (r *Runner) Views(ctx context.Context) error {
	views, err := r.pgClient.Views(ctx)
	if err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .ViewSchema | bold | green }}.{{ .Name | bold | cyan }}: {{ .Definition }}",
		Inactive: "  {{ .ViewSchema | green }}.{{ .Name | cyan }}: {{ .Definition }}",
	}

	searcher := func(input string, index int) bool {
		selected := views[index]
		name := strings.Replace(strings.ToLower(selected.Name), " ", "", -1)
		schema := strings.Replace(strings.ToLower(selected.ViewSchema), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		return strings.Contains(schema+"."+name, input)
	}
	if len(views) == 0 {
		views = append(views, postgres.View{ViewSchema: "back"})
	}
	return selecter("Views", views, searcher, templates)
}

func (r *Runner) MaterializedViews(ctx context.Context) error {
	views, err := r.pgClient.MaterializedViews(ctx)
	if err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .ViewSchema | bold | green }}.{{ .Name | bold | cyan }}: ({{ .ReferencedTableSchema }}.{{ .ReferencedTableName }}) {{ .Definition }}",
		Inactive: "  {{ .ViewSchema | green }}.{{ .Name | cyan }}: ({{ .ReferencedTableSchema }}.{{ .ReferencedTableName }}) {{ .Definition }}",
	}

	searcher := func(input string, index int) bool {
		selected := views[index]
		name := strings.Replace(strings.ToLower(selected.Name), " ", "", -1)
		schema := strings.Replace(strings.ToLower(selected.ViewSchema), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		return strings.Contains(schema+"."+name, input)
	}
	if len(views) == 0 {
		views = append(views, postgres.View{ViewSchema: "back"})
	}
	return selecter("Views", views, searcher, templates)
}

func (r *Runner) TablesBySchema(ctx context.Context) error {
	summaries, err := r.pgClient.Schemas(ctx)
	if err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Name | bold | green }}: {{ .TableCount | bold | blue}}",
		Inactive: "  {{ .Name | green }}: {{ .TableCount | blue}}",
	}

	searcher := func(input string, index int) bool {
		selected := summaries[index]
		schema := strings.Replace(strings.ToLower(selected.Name), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		return strings.Contains(schema, input)
	}
	return selecter("Tables by Schema", summaries, searcher, templates)
}

func (r *Runner) TablesBySize(ctx context.Context) error {
	tables, err := r.pgClient.TablesBySize(ctx)
	if err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Schema | bold | green }}.{{ .Name | bold | cyan }}: {{ .TableSize | bold | blue}}",
		Inactive: "  {{ .Schema | green }}.{{ .Name | cyan }}: {{ .TableSize | blue}}",
	}

	searcher := func(input string, index int) bool {
		table := tables[index]
		name := strings.Replace(strings.ToLower(table.Name), " ", "", -1)
		schema := strings.Replace(strings.ToLower(table.Schema), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		combined := schema + "." + name
		return strings.Contains(combined, input)
	}
	return selecter("Tables by TableSize", tables, searcher, templates)
}

func (r *Runner) TablesBySizeWithIndex(ctx context.Context) error {
	query := `
		SELECT schemaname as table_schema, relname as table_name,
				pg_size_pretty(pg_total_relation_size(relid)) as table_data_size,
				pg_size_pretty(pg_relation_size(relid)) as data_size,
				pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as external_size
		FROM pg_catalog.pg_statio_user_tables
		ORDER BY pg_total_relation_size(relid) desc, pg_relation_size(relid) desc`

	var tables []struct {
		PGTable
		DataSize     string `db:"data_size"`
		ExternalSize string `db:"external_size"`
	}
	if err := r.db.SelectContext(ctx, &tables, query); err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Schema | bold | green }}.{{ .Name | bold | cyan }}: {{ .TableSize | bold | blue}} (Internal {{ .IndexesSize | blue }} | External {{ .TotalSize | blue }})",
		Inactive: "  {{ .Schema | green }}.{{ .Name | cyan }}: {{ .TableSize | blue}} (Internal {{ .IndexesSize | blue }} | External {{ .TotalSize | blue }})",
	}

	searcher := func(input string, index int) bool {
		table := tables[index]
		name := strings.Replace(strings.ToLower(table.Name), " ", "", -1)
		schema := strings.Replace(strings.ToLower(table.Schema), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		combined := schema + "." + name
		return strings.Contains(combined, input)
	}
	return selecter("Tables by TableSize with Index", tables, searcher, templates)
}

func (r *Runner) TablesByRows(ctx context.Context) error {
	query := `
		SELECT n.nspname as table_schema, c.relname as table_name, c.reltuples as rows
		FROM pg_class c JOIN pg_namespace n on n.oid = c.relnamespace
		WHERE c.relkind = 'r' AND n.nspname not in ('information_schema','pg_catalog')
		ORDER BY c.reltuples desc`

	var tables []struct {
		PGTable
		Rows float64 `db:"rows"`
	}
	if err := r.db.SelectContext(ctx, &tables, query); err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Schema | bold | green }}.{{ .Name | bold | cyan }}: {{ .Rows | bold | blue}} rows",
		Inactive: "  {{ .Schema | green }}.{{ .Name | cyan }}: {{ .Rows | blue}} rows",
	}

	searcher := func(input string, index int) bool {
		table := tables[index]
		name := strings.Replace(strings.ToLower(table.Name), " ", "", -1)
		schema := strings.Replace(strings.ToLower(table.Schema), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		combined := schema + "." + name
		return strings.Contains(combined, input)
	}
	return selecter("Tables by Rows", tables, searcher, templates)
}

func (r *Runner) TablesEmpty(ctx context.Context) error {
	query := `
		SELECT n.nspname as table_schema, c.relname as table_name
		FROM pg_class c JOIN pg_namespace n on n.oid = c.relnamespace
		WHERE c.relkind = 'r' AND n.nspname not in ('information_schema','pg_catalog') AND c.reltuples = 0
		ORDER BY table_schema, table_name`

	var tables []PGTable
	if err := r.db.SelectContext(ctx, &tables, query); err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Schema | bold | green }}.{{ .Name | bold | cyan }}: 0 rows",
		Inactive: "  {{ .Schema | green }}.{{ .Name | cyan }}: 0 rows",
	}

	searcher := func(input string, index int) bool {
		table := tables[index]
		name := strings.Replace(strings.ToLower(table.Name), " ", "", -1)
		schema := strings.Replace(strings.ToLower(table.Schema), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		combined := schema + "." + name
		return strings.Contains(combined, input)
	}
	return selecter("Empty Tables", tables, searcher, templates)
}

func (r *Runner) TablesGroupByRows(ctx context.Context) error {
	query := `
		SELECT row_count, count(*) as table_count
		FROM (
			SELECT c.relname as table_name, n.nspname as table_schema,
				CASE WHEN c.reltuples > 1000000000 THEN '1b rows and more'
            		 WHEN c.reltuples > 1000000 THEN '1m - 1b rows'
					 WHEN c.reltuples > 1000 THEN '1k - 1m rows'
            		 WHEN c.reltuples > 100 THEN '100 - 1k rows'
            		 WHEN c.reltuples > 10 THEN '10 - 100 rows'
					 ELSE  '0 - 10 rows' END as row_count,
				c.reltuples as rows
			FROM pg_class c JOIN pg_namespace n on n.oid = c.relnamespace
			WHERE c.relkind = 'r' AND n.nspname not in ('pg_catalog', 'information_schema')
		) itv
		GROUP BY row_count
		ORDER BY max(rows)`

	var tables []struct {
		RowCount   string `db:"row_count"`
		TableCount int    `db:"table_count"`
	}
	if err := r.db.SelectContext(ctx, &tables, query); err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .RowCount | bold | cyan }}: {{ .TableCount | bold | blue }}",
		Inactive: "  {{ .RowCount | cyan }}: {{ .TableCount | blue }}",
	}
	return selecter("Tables Grouped By Rows", tables, nil, templates)
}

func (r *Runner) ColumnsFrequency(ctx context.Context) error {
	query := `
		SELECT c.column_name, count(*) as tables,
       		round(100.0*count(*)::decimal /
				(
					SELECT count(*)as tables
              		FROM information_schema.tables
              		WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog')
				)
       		, 2) as percent_tables
		FROM information_schema.columns c
		JOIN information_schema.tables t
     		ON t.table_schema = c.table_schema AND t.table_name = c.table_name
		WHERE t.table_type = 'BASE TABLE'
      		AND t.table_schema NOT IN ('information_schema', 'pg_catalog')
		GROUP BY c.column_name
		HAVING count(*) > 1
		ORDER BY count(*) desc;
	`

	var cols []struct {
		Name          string  `db:"column_name"`
		Tables        int     `db:"tables"`
		PercentTables float64 `db:"percent_tables"`
	}
	if err := r.db.SelectContext(ctx, &cols, query); err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Name | bold | green }}: {{ .Tables | bold | cyan }} ({{ .PercentTables | bold | blue}})",
		Inactive: "  {{ .Name | green }}: {{ .Tables | cyan }} ({{ .PercentTables | blue}})",
		Details: `
 --------- Columns ----------
 {{ "Column Name:" | faint }}	{{ .Name }}
 {{ "Table Count:" | faint }}	{{ .Tables }}
 {{ "Percentage of Tables:" | faint }}	{{ .PercentTables }}`,
	}

	searcher := func(input string, index int) bool {
		selected := cols[index]
		name := strings.Replace(strings.ToLower(selected.Name), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		return strings.Contains(name, input)
	}
	return selecter("Column Frequencies", cols, searcher, templates)
}

func (r *Runner) Version(ctx context.Context) error {
	var version string
	if err := r.db.GetContext(ctx, &version, `SELECT version()`); err != nil {
		return err
	}
	return selecter("Postgres Version", []string{version}, nil, nil)

}

func selecter(name string, items interface{}, searcher list.Searcher, templates *promptui.SelectTemplates) error {
	sel := promptui.Select{
		HideHelp:          true,
		Label:             name,
		Items:             items,
		Searcher:          searcher,
		Size:              selectSize(templates),
		StartInSearchMode: searcher != nil,
		Templates:         templates,
	}
	_, _, err := sel.Run()
	overwritePrevLine()
	return err
}

func selectSize(templates *promptui.SelectTemplates) int {
	_, height, err := terminalSize()
	if err != nil || templates == nil || height < 3 {
		return 1
	}
	return height - 4 - strings.Count(templates.Details, "\n")
}

func selectStr(label string, items []string) (string, error) {
	sel := promptui.Select{
		HideHelp: true,
		Label:    label,
		Items:    items,
		Searcher: func(input string, index int) bool {
			item := items[index]
			name := strings.Replace(strings.ToLower(item), " ", "", -1)
			input = strings.Replace(strings.ToLower(input), " ", "", -1)
			return strings.Contains(name, input)
		},
		Size:              selectSize(nil),
		StartInSearchMode: true,
	}

	_, result, err := sel.Run()
	return result, err
}

// overwritePrevLine is some shell blackmagic broken down.
func overwritePrevLine() {
	const (
		escPrevLine    = "\033[F"
		clearLine      = "2K"
		carriageReturn = "\r"
	)
	fmt.Print(escPrevLine + clearLine + carriageReturn)
}

func terminalSize() (width, height int, err error) {
	ws, err := unix.IoctlGetWinsize(int(os.Stdin.Fd()), unix.TIOCGWINSZ)
	if err != nil {
		return -1, -1, err
	}
	return int(ws.Col), int(ws.Row), nil
}
