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
package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/chzyer/readline"
	"github.com/fatih/color"
	"github.com/mattn/go-shellwords"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/model"
	"github.com/wentaojin/tidba/utils/stringutil"
)

var (
	allowedSelectQueryCommand  = "SELECT"
	allowedShowQueryCommand    = "SHOW"
	allowedUseQueryCommand     = "USE"
	allowedExplainQueryCommand = "EXPLAIN"
	allowedQueryCommands       = []string{allowedSelectQueryCommand, allowedShowQueryCommand, allowedUseQueryCommand, allowedExplainQueryCommand}
)

var cli *CommandLine

// CommandLine represents the interactive command line structure
type CommandLine struct {
	mutex             *sync.RWMutex
	prompt            string
	readliner         *readline.Instance
	promptColor       *color.Color
	rootCmd           *cobra.Command
	activeCluster     string // The name of the cluster where the interactive command line is logged in and active
	activeClusterConn *mysql.Database
	activceDbName     string // activce dbName
}

// NewCommandLine creates a new CommandLine instance
func NewCommandLine(rootCmd *cobra.Command, clusterName string, histFile string) (*CommandLine, error) {
	prompt := `tidba »»» `

	rl, err := readline.NewEx(&readline.Config{
		Prompt:            prompt,
		HistoryFile:       histFile,
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
		AutoComplete:      readline.NewPrefixCompleter(GenCompleter(rootCmd)...),
	})
	if err != nil {
		return nil, err
	}

	l := &CommandLine{
		mutex:       &sync.RWMutex{},
		prompt:      prompt,
		readliner:   rl,
		promptColor: color.New(color.FgGreen),
		rootCmd:     rootCmd,
	}

	// the new prompt is set if and only if the cluster persistentFlags is specified and the cluster is logged in.
	if clusterName != "" {
		// login cluster
		clConn, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return nil, err
		}
		l.activeClusterConn = clConn.(*mysql.Database)
		l.SetClusterName(clusterName)
	} else {
		l.SetDefaultPrompt()
	}
	return l, nil
}

// SetDefaultPrompt resets the current prompt to the default prompt as
// configured in the config.
func (l *CommandLine) SetDefaultPrompt() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.readliner.SetPrompt(l.promptColor.Sprint(`tidba »»» `))
	l.activeCluster = ""
}

// SetNewPrompt set the command line prompt
func (l *CommandLine) SetClusterPrompt() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	var b strings.Builder
	b.WriteString(l.promptColor.Sprint("tidba"))
	if l.activceDbName != "" && l.activeCluster != "" {
		b.WriteString(l.promptColor.Sprint("["))
		b.WriteString(color.New(color.FgMagenta).Sprint(l.activeCluster))
		b.WriteString(color.New(color.FgMagenta).Sprint("("))
		b.WriteString(color.New(color.FgRed).Sprintf("%s", l.activceDbName))
		b.WriteString(color.New(color.FgMagenta).Sprint(")"))
		b.WriteString(l.promptColor.Sprint("]"))
	} else if l.activceDbName == "" && l.activeCluster != "" {
		b.WriteString(l.promptColor.Sprint("["))
		b.WriteString(color.New(color.FgMagenta).Sprintf("%s", l.activeCluster))
		b.WriteString(l.promptColor.Sprint("]"))
	}
	b.WriteString(l.promptColor.Sprint(" »»» "))
	l.prompt = b.String()
	l.readliner.SetPrompt(l.prompt)
}

func (l *CommandLine) GetClusterName() string {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.activeCluster
}

func (l *CommandLine) SetDatabaseName(databaseName string) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	l.activceDbName = databaseName
}

func (l *CommandLine) SetClusterName(clusterName string) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	l.activeCluster = clusterName
}

func (l *CommandLine) ResetClusterName() {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	l.activeCluster = ""
	l.activceDbName = ""
	l.activeClusterConn = nil
}

// Run starts the interactive command line
func (l *CommandLine) Run() error {
	defer l.readliner.Close()
	printCLIASCIILogo(l.promptColor)

	var cmds []string
	// each time readline is called, it needs to re-acquire the cobra command
	getRootCmd := func() *cobra.Command {
		rootCmd := Cmd(&App{})

		// disableInteractive default hidden command
		// non-disableInteractive mode disable hidden command, display normal
		for _, subCmd := range rootCmd.Commands() {
			if subCmd.Use == "login" || subCmd.Use == "logout" || subCmd.Use == "clear" {
				subCmd.Hidden = false
			}
			cmds = append(cmds, subCmd.Use)
		}

		// when origin rootCmd persistentFlags changed, loading and new rootCmd setting
		l.rootCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
			if f.Changed {
				// running mode
				// tidba -c {clusterName} or tidba -> login -c {clusterName}
				rootCmd.PersistentFlags().Set(f.Name, f.Value.String())
			}
		})

		clusterName, err := rootCmd.PersistentFlags().GetString("cluster")
		if err != nil {
			fmt.Printf("the interactive command line get cluster flag failed: %v", err)
			os.Exit(1)
		}

		activeClusterName := l.GetClusterName()

		// when the cluster is logged in, the flag parameter clusterName is automatically set so that the cobra command can be executed normally.
		// tidba -> login -c {clusterName}
		if strings.EqualFold(clusterName, "") && !strings.EqualFold(activeClusterName, "") {
			rootCmd.PersistentFlags().Set("cluster", activeClusterName)
		}

		// hide metadata parameter settings. Only available in non-interactive mode. Cannot be changed in interactive mode.
		rootCmd.LocalFlags().MarkHidden("metadata")
		rootCmd.LocalFlags().MarkHidden("disable-interactive")

		return rootCmd
	}

	var inputBuffer []string

	// define regex match ; or \G
	re := regexp.MustCompile(`(;|\\G)`)

	for {
		// determine whether the first character of the first line belongs to command. If not, execute it as a SQL statement.
		if len(inputBuffer) > 0 {
			l.readliner.SetPrompt("    -> ") // Multi-line input prompt
		} else {
			l.SetClusterPrompt()
		}

		line, err := l.readliner.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			fmt.Printf("\n❌ Read line error: %v\n", err)
			continue
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// if you enter exit or quit, exit the program
		if line == "exit" || line == "quit" {
			fmt.Println("Bye!")
			os.Exit(0)
		}

		// clear screen
		if line == "clear" {
			if _, err := readline.ClearScreen(l.readliner); err != nil {
				return err
			}
			// write the complete input to the history
			if err := l.readliner.SaveHistory(line); err != nil {
				return fmt.Errorf("save history error: %v", err)
			}
			continue
		}

		rootCmd := getRootCmd()

		if line == "help" {
			rootCmd.SetArgs([]string{"help"})
			if err := rootCmd.Execute(); err != nil {
				fmt.Printf("\n❌ Execute command error: %v\n", err)
			}
			// write the complete input to the history
			if err := l.readliner.SaveHistory(line); err != nil {
				return fmt.Errorf("save history error: %v", err)
			}
			continue
		}

		// tidba command
		dbaCmdSli := strings.Fields(line)
		if (stringutil.IsContainStringIgnoreCase(dbaCmdSli[0], cmds) && len(inputBuffer) == 0) || (!stringutil.IsContainStringIgnoreCase(dbaCmdSli[0], allowedQueryCommands) && len(inputBuffer) == 0) {
			// command exec
			args, err := shellwords.Parse(line)
			if err != nil {
				return fmt.Errorf("parse command err: %v", err)
			}

			rootCmd.SetArgs(args)
			rootCmd.ParseFlags(args)
			if err := rootCmd.Execute(); err != nil {
				fmt.Printf("\n❌ Execute command error: %v\n", err)
			}

			// write the complete input to the history
			if err := l.readliner.SaveHistory(line); err != nil {
				return fmt.Errorf("save history error: %v", err)
			}
			continue
		}

		// append buffer
		inputBuffer = append(inputBuffer, line)

		// if the current line ends with a semicolon, the input is complete
		if strings.HasSuffix(line, ";") || strings.HasSuffix(line, "\\G") {
			if l.activeCluster == "" {
				fmt.Printf("\n❌ Execute command error: the cluster_name cannot be empty, if you need to execute the [%s] sql command, please log in to the cluster in advance by running [login -c {clusterName}]. Otherwise, run the [help] command to view.\n", strings.Join(allowedQueryCommands, "/"))
				inputBuffer = nil
				continue
			}
			fullInput := strings.Join(inputBuffer, " ")

			// write the complete input to the history
			if err := l.readliner.SaveHistory(fullInput); err != nil {
				return fmt.Errorf("save history error: %v", err)
			}

			groups, found := splitStringWithDelimiter(re, fullInput)
			if !found {
				// No ; or \\G found
				continue
			}

			// database connection not init
			if l.activeClusterConn == nil {
				clConn, err := database.Connector.GetDatabase(l.activeCluster)
				if err != nil {
					return err
				}
				l.activeClusterConn = clConn.(*mysql.Database)
			}

			for _, g := range groups {
				if g.Value == "" {
					fmt.Println("ERROR: No query specified")
					continue
				}
				queryCmd := strings.Fields(skipComments(g.Value))
				queryType := queryCmd[0]
				// to avoid operation and maintenance security, only select / show / use sql command are allowed...
				isAllowed := false
				if strings.EqualFold(queryType, allowedSelectQueryCommand) {
					isAllowed = true
				}
				if strings.EqualFold(queryType, allowedShowQueryCommand) {
					isAllowed = true
				}
				if strings.EqualFold(queryType, allowedUseQueryCommand) {
					isAllowed = true
					_, err := l.activeClusterConn.ExecContext(context.Background(), g.Value)
					if err != nil {
						fmt.Printf("❌ Execute query failed!\n\nquery error content:\n%v\n\n", err)
						inputBuffer = nil
						continue
					}
					l.SetDatabaseName(strings.Trim(queryCmd[1], ";"))
					l.SetClusterPrompt()
					fmt.Fprintln(os.Stdout, "\n✅ Database changed")
					// clear the buffer and prepare to accept the next command
					inputBuffer = nil
					continue
				}
				if strings.EqualFold(queryType, allowedExplainQueryCommand) {
					isAllowed = true
				}

				if isAllowed && !strings.EqualFold(queryType, allowedUseQueryCommand) {
					stime := time.Now()
					cols, res, err := l.activeClusterConn.GeneralQuery(context.Background(), g.Value)
					if err != nil {
						fmt.Printf("❌ Execute query failed!\n\nquery error content:\n%v\n\n", err)
						inputBuffer = nil
						continue
					}
					if g.Delimiter == "\\G" {
						if err := model.QueryResultFormatWithoutTableStyle(cols, res, time.Since(stime).Seconds()); err != nil {
							fmt.Printf("\n❌ Format result error: %v", err)
							inputBuffer = nil
							continue
						}
					} else {
						if err := model.QueryResultFormatTableStyle(cols, res, time.Since(stime).Seconds()); err != nil {
							fmt.Printf("\n❌ Format result error: %v", err)
							inputBuffer = nil
							continue
						}
					}
				} else {
					fmt.Printf("❌ Execute query failed!\n\nquery error content:\n%v\n\n", fmt.Errorf("operation and maintenance security control, only [%s] sql commands are allowed to be executed, current command [%s] are not allowed", strings.Join(allowedQueryCommands, "/"), queryType))
				}

			}

			// clear the buffer and prepare to accept the next command
			inputBuffer = nil
		}
	}
	return nil
}

func printCLIASCIILogo(c *color.Color) {
	newColor := c.SprintFunc()
	fmt.Println(newColor(`Welcome to`))
	fmt.Println(newColor(`	           _______ ___  ___  ___  `))
	fmt.Println(newColor(`	          /_  __(_) _ \/ _ )/ _ |`))
	fmt.Println(newColor(`	           / / / / // / _  / __ |`))
	fmt.Println(newColor(`	          /_/ /_/____/____/_/ |_|`))
	fmt.Println(newColor(`   ____          __  __      ___      ___                  `))
	fmt.Println(newColor(`  / __/__  __ __/ /_/ /     / _ \___ / (_)  _____ ______ __`))
	fmt.Println(newColor(` _\ \/ _ \/ // / __/ _ \   / // / -_) / / |/ / -_) __/ // /`))
	fmt.Println(newColor(`/___/\___/\_,_/\__/_//_/  /____/\__/_/_/|___/\__/_/  \_, / `))
	fmt.Println(newColor(`                                                    /___/  `))
	fmt.Println(newColor(`Type 'exit' or 'quit' for command exit. Type 'clear' to clear the current input statement.`))
	fmt.Println(newColor(`Note:                                                        `))
	fmt.Println(newColor(`-    the logged in cluster supports the execution of tidba operation commands, type 'help' for commands help.`))
	fmt.Println(newColor(`-    the logged in cluster supports the execution of SQL commands, options: [Select, Show, Use, Explain] other commands are not supported.`))
	fmt.Println(newColor(`                                                              `))

}

func printHelpASCIILogo(c *color.Color) {
	newColor := c.SprintFunc()
	fmt.Println(newColor(`Welcome to`))
	fmt.Println(newColor(`	           _______ ___  ___  ___  `))
	fmt.Println(newColor(`	          /_  __(_) _ \/ _ )/ _ |`))
	fmt.Println(newColor(`	           / / / / // / _  / __ |`))
	fmt.Println(newColor(`	          /_/ /_/____/____/_/ |_|`))
	fmt.Println(newColor(`   ____          __  __      ___      ___                  `))
	fmt.Println(newColor(`  / __/__  __ __/ /_/ /     / _ \___ / (_)  _____ ______ __`))
	fmt.Println(newColor(` _\ \/ _ \/ // / __/ _ \   / // / -_) / / |/ / -_) __/ // /`))
	fmt.Println(newColor(`/___/\___/\_,_/\__/_//_/  /____/\__/_/_/|___/\__/_/  \_, / `))
	fmt.Println(newColor(`                                                    /___/  `))
}

func GenCompleter(cmd *cobra.Command) []readline.PrefixCompleterInterface {
	pc := []readline.PrefixCompleterInterface{}
	if len(cmd.Commands()) != 0 {
		for _, v := range cmd.Commands() {
			if v.HasFlags() {
				flagsPc := []readline.PrefixCompleterInterface{}
				flagUsages := strings.Split(strings.Trim(v.Flags().FlagUsages(), " "), "\"n")
				for i := 0; i < len(flagUsages)-1; i++ {
					flagsPc = append(flagsPc, readline.PcItem(strings.Split(strings.Trim(flagUsages[i], " "), " ")[0]))
				}
				flagsPc = append(flagsPc, GenCompleter(v)...)
				pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], flagsPc...))

			} else {
				pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], GenCompleter(v)...))
			}
		}
	}
	return pc
}

// Define a structure to store groups and their corresponding separators
// Used to sql command
type Group struct {
	Value     string // group value
	Delimiter string // separators (; 或者 \G)
}

func splitStringWithDelimiter(re *regexp.Regexp, input string) ([]Group, bool) {
	matches := re.FindAllStringIndex(input, -1)

	if len(matches) == 0 {
		// No ; or \\G found
		return nil, false
	}

	var result []Group
	lastIndex := 0
	for _, match := range matches {
		// Extract the part before the match
		result = append(result, Group{
			Value:     input[lastIndex:match[0]],
			Delimiter: input[match[0]:match[1]], // 记录分隔符
		})
		lastIndex = match[1] // Update lastIndex to the matching index
	}

	// Handle the last part (after the last delimiter)
	lastValue := strings.TrimSpace(input[lastIndex:])
	if lastValue != "" {
		result = append(result, Group{
			Value:     lastValue,
			Delimiter: "", // No delimiter for the last part
		})
	}

	return result, true
}

// Skip comments (-- for single-line and /* */ for block comments)
func skipComments(input string) string {
	var result strings.Builder
	inBlockComment := false
	lines := strings.Split(input, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if inBlockComment {
			// Handle the end of a block comment */
			if idx := strings.Index(line, "*/"); idx != -1 {
				inBlockComment = false
				line = line[idx+2:] // Skip the block comment part
			} else {
				continue // The entire line is inside a block comment, skip it
			}
		}

		// Handle single-line comments --
		if idx := strings.Index(line, "--"); idx != -1 {
			line = line[:idx] // Keep only the part before --
		}

		// Handle block comments /*
		if idx := strings.Index(line, "/*"); idx != -1 {
			endIdx := strings.Index(line, "*/")
			if endIdx == -1 {
				// Block comment spans multiple lines
				inBlockComment = true
				line = line[:idx] // Keep only the part before /*
			} else {
				// Block comment ends on the same line
				line = line[:idx] + line[endIdx+2:]
			}
		}

		// Append the processed line to the result
		result.WriteString(line)
	}

	return strings.TrimSpace(result.String())
}
