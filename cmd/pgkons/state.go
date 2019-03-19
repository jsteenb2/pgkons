package main

import (
	"context"
	"strings"

	"github.com/manifoldco/promptui"
)

type StateFn func(context.Context, *Runner) (StateFn, error)

type state struct {
	Name string
	Fn   StateFn
}

var (
	startStates = []state{exploreState, playgroundState}

	startState = state{
		Name: "Back to Start",
		Fn: func(context.Context, *Runner) (StateFn, error) {
			return selectState("Options", startStates...)
		},
	}

	schemaState = state{
		Name: "Schemas",
		Fn: func(ctx context.Context, r *Runner) (StateFn, error) {
			all := state{
				Name: "All",
				Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.Schemas(ctx) },
			}
			userCreated := state{
				Name: "User Created",
				Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.SchemasUserCreated(ctx) },
			}
			return selectState("Schema Options", all, userCreated)
		},
	}

	tableState = state{
		Name: "Tables",
		Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.Tables(ctx) },
	}

	viewState = state{
		Name: "Views",
		Fn: func(ctx context.Context, r *Runner) (StateFn, error) {
			all := state{
				Name: "All",
				Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.Views(ctx) },
			}
			materialized := state{
				Name: "Materialized",
				Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.MaterializedViews(ctx) },
			}
			return selectState("Views", all, materialized)
		},
	}

	statsState = state{
		Name: "Stats",
		Fn: func(ctx context.Context, r *Runner) (StateFn, error) {
			statStates := []state{
				{
					Name: "Table Count Per Schema",
					Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.TablesBySchema(ctx) },
				},
				{
					Name: "Tables By Size",
					Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.TablesBySize(ctx) },
				},
				{
					Name: "Tables By Size With Indexes",
					Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.TablesBySizeWithIndex(ctx) },
				},
				{
					Name: "Table Row Counts",
					Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.TablesByRows(ctx) },
				},
				{
					Name: "Empty Tables",
					Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.TablesEmpty(ctx) },
				},
				{
					Name: "Tables Grouped By Rows",
					Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.TablesGroupByRows(ctx) },
				},
				{
					Name: "Column Name Frequencies",
					Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.ColumnsFrequency(ctx) },
				},
				{
					Name: "Postgres Version",
					Fn:   func(ctx context.Context, r *Runner) (StateFn, error) { return nil, r.Version(ctx) },
				},
			}
			return selectState("Stats", statStates...)
		},
	}

	exploreState = state{
		Name: "Explore",
		Fn: func(ctx context.Context, r *Runner) (StateFn, error) {
			return selectState("Where too?", schemaState, tableState, viewState, statsState)
		},
	}

	playgroundState = state{
		Name: "PlayGround",
		Fn: func(ctx context.Context, r *Runner) (StateFn, error) {
			return nil, nil
		},
	}
)

func selectState(name string, states ...state) (StateFn, error) {
	if len(states) == 0 {
		return nil, nil
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}",
		Active:   "» {{ .Name | bold | cyan }} ",
		Inactive: "  {{ .Name | cyan }} ",
		Selected: "{{ print .Name }}",
	}

	searcher := func(input string, index int) bool {
		table := states[index]
		name := strings.Replace(strings.ToLower(table.Name), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		return strings.Contains(name, input)
	}

	sel := promptui.Select{
		HideHelp:          true,
		Label:             name,
		Items:             states,
		Searcher:          searcher,
		Size:              selectSize(templates),
		StartInSearchMode: true,
		Templates:         templates,
	}

	i, _, err := sel.Run()
	if err != nil {
		return nil, err
	}
	overwritePrevLine()

	return states[i].Fn, nil
}
