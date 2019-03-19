package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/manifoldco/promptui/list"

	"golang.org/x/sys/unix"

	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
	"github.com/manifoldco/promptui"
)

var debug = flag.Bool("debug", false, "turn debug on to view corresponding errors and what not in the console")

var errFinished = errors.New("program to exit")

func main() {
	connect, err := newDBCFG()
	if err != nil {
		if *debug {
			log.Println(err)
			os.Exit(1)
		}
	}
	db, err := sql.Open("postgres", connect)
	if err != nil {
		check(err)
		os.Exit(1)
	}
	defer func() {
		check(db.Close())
	}()

	ctx := systemCtx()

	runner := NewRunner(db)
	err = runner.Run(ctx, *debug)
	if *debug && err != nil &&
		err != context.Canceled &&
		err != errFinished {
		log.Println(err)
	}
}

func validateEmptyInput(label string) func(input string) error {
	return func(input string) error {
		if input == "" {
			return errors.New("must provide a " + label)
		}
		return nil
	}
}

type (
	schema struct {
		Name        string `db:"schema_name"`
		Owner       string `db:"schema_owner"`
		CatalogName string `db:"catalog_name"`
	}

	pgTable struct {
		Catalog string `db:"table_catalog"`
		Name    string `db:"table_name"`
		Owner   string `db:"table_type"`
		Schema  string `db:"table_schema"`
		Size    string `db:"table_data_size"`
	}

	view struct {
		Name                  string `db:"view_name"`
		Definition            string `db:"definition"`
		IsPopulated           bool   `db:"ispopulated"`
		Owner                 string `db:"owner"`
		ViewSchema            string `db:"schema_name"`
		ReferencedTableSchema string `db:"referenced_table_schema"`
		ReferencedTableName   string `db:"referenced_table_name"`
	}
)

type Runner struct {
	db *sqlx.DB
}

func NewRunner(db *sql.DB) *Runner {
	return &Runner{
		db: sqlx.NewDb(db, "postgres"),
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

func (r *Runner) Schemas(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT schema_name, schema_owner, catalog_name
		FROM information_schema.schemata
		ORDER BY schema_owner, schema_name`

	var schemas []schema
	if err := r.db.SelectContext(ctx, &schemas, query); err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Name | bold|  cyan }} ({{ .Owner | bold | red }})",
		Inactive: "  {{ .Name | cyan }} ({{ .Owner | red }})",
		Details: `
--------- Schema ----------
{{ "Name:" | faint }}	{{ .Name }}
{{ "Owner:" | faint }}	{{ .Owner }}
{{ "Catalog name:" | faint }}	{{ .CatalogName }}`,
	}

	searcher := func(input string, index int) bool {
		sch := schemas[index]
		name := strings.Replace(strings.ToLower(sch.Name), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		return strings.Contains(name, input)
	}
	return selecter("Schemas", schemas, searcher, templates)
}

func (r *Runner) SchemasUserCreated(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT s.nspname as schema_name, u.usename as schema_owner
		FROM pg_catalog.pg_namespace s JOIN pg_catalog.pg_user u on u.usesysid = s.nspowner
		WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'public')
      		AND nspname not like 'pg_toast%'
      		AND nspname not like 'pg_temp_%'
		ORDER BY schema_name;
	`

	var schemas []schema
	if err := r.db.SelectContext(ctx, &schemas, query); err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Name | bold|  cyan }} ({{ .Owner | bold | red }})",
		Inactive: "  {{ .Name | cyan }} ({{ .Owner | red }})",
		Details: `
--------- Schema ----------
{{ "Name:" | faint }}	{{ .Name }}
{{ "Owner:" | faint }}	{{ .Owner }}`,
	}

	searcher := func(input string, index int) bool {
		sch := schemas[index]
		name := strings.Replace(strings.ToLower(sch.Name), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		return strings.Contains(name, input)
	}
	return selecter("Schemas", schemas, searcher, templates)
}

func (r *Runner) Tables(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT table_schema, table_name, table_catalog, table_type
		FROM information_schema.tables
		ORDER BY table_schema, table_name`

	var tables []pgTable
	err := r.db.SelectContext(ctx, &tables, query)
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
 {{ "Schema:" | faint }}	{{ .Schema }}
 {{ "Catalog:" | faint }}	{{ .Catalog }}`,
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
	if table == "" {
		return errors.New("no table provided")
	}

	query := `
		SELECT table_catalog, table_schema, table_name, column_name, is_nullable, data_type
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE table_name = $1`

	type description struct {
		Catalog  string `db:"table_catalog"`
		Schema   string `db:"table_schema"`
		Name     string `db:"table_name"`
		Column   string `db:"column_name"`
		Nullable string `db:"is_nullable"`
		Type     string `db:"data_type"`
	}

	var descs []description
	err := r.db.SelectContext(ctx, &descs, query, table)
	if err != nil {
		return err
	}

	return nil
}

func (r *Runner) Views(ctx context.Context) error {
	query := `
		SELECT u.view_schema as schema_name, u.view_name, u.table_schema as referenced_table_schema,
       			u.table_name as referenced_table_name, v.view_definition as definition
		FROM information_schema.view_table_usage u
		JOIN information_schema.views v ON u.view_schema = v.table_schema AND u.view_name = v.table_name
		WHERE u.table_schema NOT IN ('information_schema', 'pg_catalog')
		ORDER BY u.view_schema, u.view_name;`

	var views []view
	if err := r.db.SelectContext(ctx, &views, query); err != nil {
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
		views = append(views, view{ViewSchema: "back"})
	}
	return selecter("Views", views, searcher, templates)
}

func (r *Runner) MaterializedViews(ctx context.Context) error {
	query := `
		SELECT schemaname as schema_name, matviewname as view_name, matviewowner as owner, ispopulated, definition
		FROM pg_matviews
		ORDER BY schema_name, view_name`

	var views []view
	if err := r.db.SelectContext(ctx, &views, query); err != nil {
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
		views = append(views, view{ViewSchema: "back"})
	}
	return selecter("Views", views, searcher, templates)
}

func (r *Runner) TablesBySchema(ctx context.Context) error {
	query := `
		SELECT table_schema, count(*) as tables
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		GROUP BY table_schema
		ORDER BY table_schema;
	`

	var summaries []struct {
		Schema     string `db:"table_schema"`
		TableCount int    `db:"tables"`
	}
	if err := r.db.SelectContext(ctx, &summaries, query); err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Schema | bold | green }}: {{ .TableCount | bold | blue}}",
		Inactive: "  {{ .Schema | green }}: {{ .TableCount | blue}}",
	}

	searcher := func(input string, index int) bool {
		selected := summaries[index]
		schema := strings.Replace(strings.ToLower(selected.Schema), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		return strings.Contains(schema, input)
	}
	return selecter("Tables by Schema", summaries, searcher, templates)
}

func (r *Runner) TablesBySize(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT schemaname as table_schema,
				relname as table_name,
				pg_size_pretty(pg_relation_size(relid)) as table_data_size
		FROM pg_catalog.pg_statio_user_tables
		ORDER BY pg_relation_size(relid) desc`

	var tables []pgTable
	err := r.db.SelectContext(ctx, &tables, query)
	if err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Schema | bold | green }}.{{ .Name | bold | cyan }}: {{ .Size | bold | blue}}",
		Inactive: "  {{ .Schema | green }}.{{ .Name | cyan }}: {{ .Size | blue}}",
	}

	searcher := func(input string, index int) bool {
		table := tables[index]
		name := strings.Replace(strings.ToLower(table.Name), " ", "", -1)
		schema := strings.Replace(strings.ToLower(table.Schema), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		combined := schema + "." + name
		return strings.Contains(combined, input)
	}
	return selecter("Tables by Size", tables, searcher, templates)
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
		pgTable
		DataSize     string `db:"data_size"`
		ExternalSize string `db:"external_size"`
	}
	if err := r.db.SelectContext(ctx, &tables, query); err != nil {
		return err
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Schema | bold | green }}.{{ .Name | bold | cyan }}: {{ .Size | bold | blue}} (Internal {{ .DataSize | blue }} | External {{ .ExternalSize | blue }})",
		Inactive: "  {{ .Schema | green }}.{{ .Name | cyan }}: {{ .Size | blue}} (Internal {{ .DataSize | blue }} | External {{ .ExternalSize | blue }})",
	}

	searcher := func(input string, index int) bool {
		table := tables[index]
		name := strings.Replace(strings.ToLower(table.Name), " ", "", -1)
		schema := strings.Replace(strings.ToLower(table.Schema), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		combined := schema + "." + name
		return strings.Contains(combined, input)
	}
	return selecter("Tables by Size with Index", tables, searcher, templates)
}

func (r *Runner) TablesByRows(ctx context.Context) error {
	query := `
		SELECT n.nspname as table_schema, c.relname as table_name, c.reltuples as rows
		FROM pg_class c JOIN pg_namespace n on n.oid = c.relnamespace
		WHERE c.relkind = 'r' AND n.nspname not in ('information_schema','pg_catalog')
		ORDER BY c.reltuples desc`

	var tables []struct {
		pgTable
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

	var tables []pgTable
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
	if err != nil || templates == nil || height < 4 {
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

func check(err error) {
	if err != nil && *debug {
		log.Println(err)
	}
}

func systemCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Kill, os.Interrupt)
	go func() {
		<-sigs
		cancel()
	}()
	return ctx
}
