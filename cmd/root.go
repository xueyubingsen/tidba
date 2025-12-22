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
	"reflect"
	"time"

	"github.com/chzyer/readline"
	"github.com/fatih/color"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/sqlite"
	"github.com/wentaojin/tidba/utils/stringutil"
	"github.com/wentaojin/tidba/utils/version"
)

func init() {
	database.Connector = database.NewDBConnector()
}

type App struct {
	metadata           string
	clusterName        string
	disableInteractive bool
	version            bool
	history            string
}

/*
Use the SilenceErrors and SilenceUsage properties. These two properties can suppress repeated error messages and usage information.

	SilenceErrors: When set to true, Cobra will not print error messages to standard output.
	SilenceUsage: When set to true, Cobra will not print usage information to standard output

Cobra PersistentPreRunE hook function integration rules, the sub-command's PersistentPreRunE will overwrite the parent command's PersistentPreRunE, that is, if the parent command and the sub-command configure PersistentPreRunE at the same time, the sub-command will take effect, and the parent command will not take effect. You need to explicitly call

	sucommand call parent PersistentPreRunE command:
		if parent := cmd.Parent(); parent != nil && parent.PersistentPreRunE != nil {
	        parent.PersistentPreRunE(cmd, args)
	    }

	sucommand call root PersistentPreRunE command :
		if err := cmd.Root().PersistentPreRunE(cmd, args); err != nil {
			return err
		}
*/
func (a *App) Cmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:  "tidba",
		Long: "TiDBA (tidba) is a CLI for tidb distributed data dba operation and maintenance, which can quickly analyze, diagnose and troubleshoot problems.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			dir, err := homedir.Expand(a.metadata)
			if err != nil {
				return err
			}
			err = stringutil.PathNotExistOrCreate(dir)
			if err != nil {
				return err
			}

			if _, ok := database.Connector.LoadDatabase(database.DefaultSqliteClusterName); !ok {
				connector, err := database.CreateConnector(context.Background(), &database.ClusterConfig{
					DbType: database.DatabaseTypeSqlite,
					DSN:    dir,
				})
				if err != nil {
					return err
				}
				database.Connector.AddDatabase(database.DefaultSqliteClusterName, connector)
			}

			// license skip Limits
			if cmd.Use == "license" || cmd.Use == "generate" || cmd.Use == "activate" {
				return nil
			}

			// if built with -tags nolicense, skip license verification entirely (for tests)
			if !IsLicenseCheckEnabled() {
				a.history = fmt.Sprintf("%s/tidba_history", dir)
				return nil
			}

			macAddr, err := getDefaultMACAddress()
			if err != nil {
				return err
			}
			db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
			if err != nil {
				return err
			}
			lic, err := db.(*sqlite.Database).GetLicense(context.Background(), macAddr)
			if err != nil {
				return err
			}
			if reflect.DeepEqual(lic, &sqlite.License{}) {
				return fmt.Errorf("the software is not authorized. please use [tidba license generate --user {userName} --day {licenseDays}] to generate the authorization code, and then contact the author to obtain the authorization key to activate the software")
			}

			expir, err := time.ParseInLocation("2006-01-02 15:04:05", lic.ExpireTime, time.Local)
			if err != nil {
				return err
			}
			if time.Now().In(time.Local).After(expir) {
				return fmt.Errorf("the license has expired and cannot be used, please contact the author for reactivation")
			}
			// verify MAC
			if macAddr != lic.MacAddress {
				return fmt.Errorf("the server address does not match. please use it on the original activation server")
			}

			a.history = fmt.Sprintf("%s/tidba_history", dir)
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if a.version {
				fmt.Println(version.GetRawVersionInfo())
				return nil
			}

			if !a.disableInteractive {
				var err error
				cli, err = NewCommandLine(
					cmd,
					a.clusterName,
					a.history)
				if err != nil {
					return err
				}
				return cli.Run()
			}
			return cmd.Help()
		},
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	rootCmd.PersistentFlags().StringVarP(&a.metadata, "metadata", "M", "~/.tidba", "location of the tidba metadata database")
	rootCmd.PersistentFlags().StringVarP(&a.clusterName, "cluster", "c", "", "configure the cluster name that tidba needs to operate")
	rootCmd.Flags().BoolVarP(&a.disableInteractive, "disable-interactive", "d", false, "interactive for the tidba application (default: interactive mode)")
	rootCmd.Flags().BoolVarP(&a.version, "version", "v", false, "version for the tidba application")

	rootCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fgGreen := color.New(color.FgGreen)
		printHelpASCIILogo(fgGreen)
		fmt.Println(fgGreen.Sprint(cmd.UsageString()))
	})
	return rootCmd
}

type AppClear struct {
	*App
}

func (a *App) AppClear() Cmder {
	return &AppClear{App: a}
}

func (a *AppClear) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clear",
		Short: "clear screen operation（only interactive mode）",
		Long:  `Options for the terminal screen clear operation（only interactive mode）`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !a.disableInteractive {
				if _, err := readline.ClearScreen(cli.readliner); err != nil {
					return err
				}
			}
			return nil
		},
		SilenceErrors: true,
		SilenceUsage:  true,
		Hidden:        true,
	}
	return cmd
}
