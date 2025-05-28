/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package sql

import (
	"context"
	"fmt"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/lipgloss"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/database/sqlite"
	"github.com/wentaojin/tidba/model"
)

type SqlQueryModel struct {
	ctx           context.Context
	cancel        context.CancelFunc
	clusterName   string
	nearly        int
	enableHistory bool
	startTime     string
	endTime       string
	command       string
	schemaName    string
	sqlDigest     string
	trend         int
	spinner       spinner.Model
	mode          string
	Msgs          interface{}
	Error         error
}

func NewSqlQueryModel(clusterName string, nearly int,
	enableHistory bool,
	startTime string,
	endTime string,
	command string,
	schemaName string,
	sqlDigest string,
	trend int,
) SqlQueryModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return SqlQueryModel{
		ctx:           ctx,
		cancel:        cancel,
		spinner:       sp,
		clusterName:   clusterName,
		nearly:        nearly,
		enableHistory: enableHistory,
		startTime:     startTime,
		endTime:       endTime,
		command:       command,
		schemaName:    schemaName,
		sqlDigest:     sqlDigest,
		trend:         trend,
		mode:          model.BubblesModeQuering,
	}
}

func (m SqlQueryModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m SqlQueryModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd tea.Cmd
	)
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			m.cancel()
			return m, tea.Quit
		default:
			return m, nil
		}
	case listRespMsg:
		m.mode = model.BubblesModeQueried
		if msg.err != nil {
			m.Error = msg.err
		} else {
			m.Msgs = msg.msgs
		}
		return m, tea.Quit
	case spinner.TickMsg:
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	default:
		m.spinner, cmd = m.spinner.Update(msg)
		switch m.command {
		case "DISPLAY":
			return m, tea.Batch(
				cmd,
				submitSqlData(m.ctx, m.clusterName, m.nearly, m.enableHistory, m.startTime, m.endTime, m.sqlDigest, m.trend), // submit list data
			)
		case "QUERY":
			return m, tea.Batch(
				cmd,
				submitBindQueryData(m.ctx, m.clusterName, m.schemaName, m.sqlDigest), // submit list data
			)
		case "DELETE":
			return m, tea.Batch(
				cmd,
				submitBindDeleteData(m.ctx, m.clusterName, m.schemaName, m.sqlDigest), // submit list data
			)
		default:
			return m, tea.Quit
		}
	}
}

func (m SqlQueryModel) View() string {
	switch m.mode {
	case model.BubblesModeQuering:
		return fmt.Sprintf(
			"%s Quering cluster sql information...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	default:
		if m.Error != nil {
			return "❌ Queried failed!\n\n"
		}
		return "✅ Queried successfully!\n\n"
	}
}

type listRespMsg struct {
	msgs interface{}
	err  error
}

type QueriedRespMsg struct {
	QueriedSummary      *QueriedResultMsg
	QueriedPlanSummary  *QueriedResultMsg
	QueriedTrendSummary *QueriedResultMsg
	QueriedPlanDetail   []*QueiredPlanMsg
}

type QueriedResultMsg struct {
	Columns []string
	Results [][]interface{}
}

type QueiredPlanMsg struct {
	SampleUser string
	SchemaName string
	PlanDigest string
	AvgLatency float64
	SqlText    string
	SqlPlan    string
}

func submitSqlData(ctx context.Context, clusterName string, nearly int,
	enableHistory bool,
	startTime string,
	endTime string,
	sqlDigest string,
	trend int) tea.Cmd {
	return func() tea.Msg {
		queries, err := SqlDisplayQuery(ctx, clusterName, nearly, startTime, endTime, enableHistory, sqlDigest, trend)
		if err != nil {
			return listRespMsg{err: err}
		}
		return listRespMsg{msgs: queries, err: nil}
	}
}

func submitBindQueryData(ctx context.Context, clusterName string, schemaName, sqlDigest string) tea.Cmd {
	return func() tea.Msg {
		metaDB, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return listRespMsg{err: err}
		}
		meta := metaDB.(*sqlite.Database)

		binds, err := meta.FindSqlBinding(ctx, clusterName)
		if err != nil {
			return listRespMsg{err: err}
		}

		if len(binds) == 0 {
			return listRespMsg{msgs: "metadata not found query binding records Or the binding record was not created by tidba, please see by [select * from mysql.bind_info order by create_time desc limit 5]", err: nil}
		}
		var bs []*sqlite.SqlBinding
		switch {
		case schemaName == "" && sqlDigest != "":
			for _, b := range binds {
				if b.ClusterName == clusterName && b.SqlDigest == sqlDigest {
					bs = append(bs, b)
				}
			}
		case schemaName == "" && sqlDigest == "":
			bs = binds
		case schemaName != "" && sqlDigest != "":
			for _, b := range binds {
				if b.ClusterName == clusterName && b.SchemaName == schemaName && b.SqlDigest == sqlDigest {
					bs = append(bs, b)
				}
			}
		default:
			for _, b := range binds {
				if b.ClusterName == clusterName && b.SchemaName == schemaName {
					bs = append(bs, b)
				}
			}
		}

		var rows [][]interface{}
		for _, b := range bs {
			var row []interface{}
			row = append(row, b.ID)
			row = append(row, b.SchemaName)
			row = append(row, b.SqlDigest)
			row = append(row, b.DigestText)
			row = append(row, b.OptimizeText)
			row = append(row, b.CreatedAt)
			rows = append(rows, row)
		}

		return listRespMsg{msgs: &QueriedResultMsg{
			Columns: []string{"ID", "SCHEMA_NAME", "SQL_DIGEST", "DIGEST_TEXT", "OPTIMIZE_TEXT", "CREATE_TIME"},
			Results: rows,
		}, err: nil}
	}
}

func submitBindDeleteData(ctx context.Context, clusterName string, schemaName, sqlDigest string) tea.Cmd {
	return func() tea.Msg {
		metaDB, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return listRespMsg{err: err}
		}
		meta := metaDB.(*sqlite.Database)

		binds, err := meta.FindSqlBinding(ctx, clusterName)
		if err != nil {
			return listRespMsg{err: err}
		}

		if len(binds) == 0 {
			return listRespMsg{msgs: "metadata not found query binding records Or the binding record was not created by tidba, please see by [select * from mysql.bind_info order by create_time desc limit 5]", err: nil}
		}
		var bs []*sqlite.SqlBinding
		if schemaName == "" {
			for _, b := range binds {
				if b.ClusterName == clusterName && b.SqlDigest == sqlDigest {
					bs = append(bs, b)
				}
			}
		} else {
			for _, b := range binds {
				if b.ClusterName == clusterName && b.SchemaName == schemaName && b.SqlDigest == sqlDigest {
					bs = append(bs, b)
				}
			}
		}

		connDB, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return err
		}
		db := connDB.(*mysql.Database)

		var rows [][]interface{}

		for _, b := range bs {
			var drops []string
			if b.SchemaName == "*" {
				drops = append(drops, fmt.Sprintf("DROP GLOBAL BINDING FOR %s", b.DigestText))
			} else {
				drops = append(drops, fmt.Sprintf("USE %s", b.SchemaName))
				drops = append(drops, fmt.Sprintf("DROP GLOBAL BINDING FOR %s", b.DigestText))
			}
			for _, d := range drops {
				_, err := db.ExecContext(ctx, d)
				if err != nil {
					return listRespMsg{err: err}
				}
			}
			if _, err := meta.DeleteSqlBinding(ctx, clusterName, b.SchemaName, b.SqlDigest); err != nil {
				return listRespMsg{err: err}
			}

			var row []interface{}
			row = append(row, b.ID)
			row = append(row, b.SchemaName)
			row = append(row, b.SqlDigest)
			row = append(row, b.DigestText)
			row = append(row, b.OptimizeText)
			row = append(row, b.CreatedAt)
			rows = append(rows, row)
		}
		return listRespMsg{msgs: &QueriedResultMsg{
			Columns: []string{"ID", "SCHEMA_NAME", "SQL_DIGEST", "DIGEST_TEXT", "OPTIMIZE_TEXT", "CREATE_TIME"},
			Results: rows,
		}, err: nil}
	}
}
