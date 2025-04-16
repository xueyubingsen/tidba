package main

import (
	"fmt"
	"os"

	"github.com/atotto/clipboard"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
)

type model struct {
	textarea textarea.Model
}

func initialModel() model {
	ta := textarea.New()
	ta.SetHeight(30)
	ta.SetWidth(120)
	ta.ShowLineNumbers = false
	ta.CharLimit = 0
	ta.Focus()
	return model{
		textarea: ta,
	}
}

func (m model) Init() tea.Cmd {
	return textarea.Blink
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "esc":
			return m, tea.Quit
		case "ctrl+a":
			m.textarea.SetCursor(0)
			// Select all text by setting the end cursor position to the end of the text
			m.textarea.SetCursor(len(m.textarea.Value()))
		case "ctrl+x":
			// Copy selected text to clipboard
			clipboard.WriteAll(m.textarea.Value())
			// Clear the textarea
			m.textarea.SetValue("")
		case "ctrl+v":
			// Paste text from clipboard
			pasteText, _ := clipboard.ReadAll()
			m.textarea.InsertString(pasteText)
		case "ctrl+d":
			m.textarea.SetValue("")
		}
	}

	m.textarea, cmd = m.textarea.Update(msg)
	return m, cmd
}

func (m model) View() string {
	return fmt.Sprintf(
		"Text Area\n\n%s\n\n%s",
		m.textarea.View(),
		"ctrl+a=Select All • ctrl+x=Cut • ctrl+v=Paste • ctrl+d=Delete All",
	)
}

func main() {
	p := tea.NewProgram(initialModel())
	if err := p.Start(); err != nil {
		fmt.Printf("Alas, there's been an error: %v", err)
		os.Exit(1)
	}
}
