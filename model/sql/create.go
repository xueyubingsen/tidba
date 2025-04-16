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

	"github.com/atotto/clipboard"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/wentaojin/tidba/model"
)

type SqlBindCreateModel struct {
	ctx           context.Context
	cancel        context.CancelFunc
	clusterName   string
	nearly        int
	enableHistory bool
	startTime     string
	endTime       string
	schemaName    string
	sqlDigest     string
	spinner       spinner.Model
	textarea      textarea.Model
	mode          string
	Msgs          interface{}
	Error         error
}

func NewSqlBindCreateModel(clusterName string, nearly int,
	enableHistory bool,
	startTime string,
	endTime string,
	schemaName string,
	sqlDigest string,
) SqlBindCreateModel {
	ta := textarea.New()
	ta.SetHeight(30)
	ta.SetWidth(120)
	ta.ShowLineNumbers = false
	ta.CharLimit = 0
	ta.Focus()

	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return SqlBindCreateModel{
		ctx:           ctx,
		cancel:        cancel,
		spinner:       sp,
		textarea:      ta,
		clusterName:   clusterName,
		nearly:        nearly,
		enableHistory: enableHistory,
		startTime:     startTime,
		endTime:       endTime,
		schemaName:    schemaName,
		sqlDigest:     sqlDigest,
		mode:          model.BubblesModeEditing,
	}
}

func (m SqlBindCreateModel) Init() tea.Cmd {
	return tea.Batch(
		textarea.Blink, // the original cursor is blinking
		m.spinner.Tick, // added spinner animation
	)
}

func (m SqlBindCreateModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyEsc:
			if m.textarea.Focused() {
				m.textarea.Blur()
			}
		case tea.KeyCtrlC:
			m.cancel()
			return m, tea.Quit
		case tea.KeyCtrlS: // ctrl + S submit shortcut key
			m.mode = model.BubblesModeSubmitting
			vals := m.textarea.Value()
			if vals == "" {
				cmd = m.textarea.Focus()
				return m, tea.Batch(
					cmd,
					m.spinner.Tick,
					func() tea.Msg {
						return listRespMsg{err: fmt.Errorf("optimize sql text context cannot be null, please input")}
					},
				)
			}
			return m, tea.Batch(
				submitBindCreateData(m.ctx, m.clusterName, m.nearly, m.enableHistory, m.startTime, m.endTime, m.schemaName, m.sqlDigest, vals),
				m.spinner.Tick, // keep spinner animation
			)
		case tea.KeyCtrlV:
			// Paste text from clipboard
			pasteText, _ := clipboard.ReadAll()
			m.textarea.InsertString(pasteText)
		case tea.KeyCtrlD:
			m.textarea.SetValue("")
		default:
			var cmds []tea.Cmd
			if !m.textarea.Focused() {
				cmd = m.textarea.Focus()
				cmds = append(cmds, cmd)
			}
			m.textarea, cmd = m.textarea.Update(msg)
			cmds = append(cmds, cmd)
			return m, tea.Batch(cmds...)
		}
	case spinner.TickMsg:
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case listRespMsg:
		if msg.err != nil {
			m.mode = model.BubblesModeEditing
			m.Error = msg.err
			// regain focus, refill error data, and re-edit
			cmd = m.textarea.Focus()
			return m, cmd
		} else {
			m.mode = model.BubblesModeSubmitted
			m.Msgs = msg.msgs
			return m, tea.Quit
		}
	}

	return m, nil
}

func (m SqlBindCreateModel) View() string {
	switch m.mode {
	case model.BubblesModeSubmitting:
		return fmt.Sprintf(
			"%s Create query binding...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	case model.BubblesModeSubmitted:
		return "✅ Created binding successfully!\n\n"
	default:
		errorView := ""
		if m.Error != nil {
			errorView = fmt.Sprintf("\n❌ Created binding error: %s\n", m.Error.Error())
		}
		return fmt.Sprintf(
			"Edit your query optimize text (ctrl+s to submit)\n\n%s\n%s\n%s\n",
			m.textarea.View(),
			errorView,
			"(ctrl+c=Quit • ctrl+v=Paste • ctrl+d=Delete)",
		)
	}
}

func submitBindCreateData(ctx context.Context, clusterName string, nearly int,
	enableHistory bool,
	startTime string,
	endTime string,
	schemaName, sqlDigest, sqlDigestText string) tea.Cmd {
	return func() tea.Msg {
		if err := SqlQueryCreateBind(ctx, clusterName, nearly, startTime, endTime, enableHistory, schemaName, sqlDigest, sqlDigestText); err != nil {
			return listRespMsg{err: err}
		}

		return listRespMsg{msgs: "Binding sql created! please see by [select * from mysql.bind_info order by create_time desc limit 5]", err: nil}
	}
}
