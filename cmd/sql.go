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
	"fmt"
	"reflect"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/model"
	"github.com/wentaojin/tidba/model/sql"
)

type AppSql struct {
	*App

	nearly        int
	startTime     string
	endTime       string
	sqlDigest     string
	enableSql     bool
	enableHistory bool
}

func (a *App) AppSql() Cmder {
	return &AppSql{App: a}
}

func (a *AppSql) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql",
		Short: "SQL used to perform execution plan related operations on a certain type of sql digest",
		Long:  "SQL used to perform execution plan related operations on a certain type of sql digest where the specified cluster name is located",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.Help(); err != nil {
				return err
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}

	cmd.PersistentFlags().IntVar(&a.nearly, "nearly", 30, "configure the cluster database query time windows, size: minutes")
	cmd.PersistentFlags().StringVar(&a.startTime, "start", "", "configure the cluster database query range with start time")
	cmd.PersistentFlags().StringVar(&a.endTime, "end", "", "configure the cluster database query range with end time")
	cmd.PersistentFlags().BoolVar(&a.enableSql, "enable-sql", false, "configure the cluster database query result display sql_text if setting enable_sql")
	cmd.PersistentFlags().StringVar(&a.sqlDigest, "sql-digest", "", "configure the cluster database sql digest")
	cmd.PersistentFlags().BoolVar(&a.enableHistory, "enable-history", false, "configure the cluster database query system cluster_statements_summary if enable history")
	return cmd
}

type AppSqlDisplay struct {
	*AppSql
	trend int
}

func (a *AppSql) AppSqlDisplay() Cmder {
	return &AppSqlDisplay{AppSql: a}
}

func (a *AppSqlDisplay) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "display",
		Short: "Display used to display sql digest corresponding sql and execution plan related information",
		Long:  "Display used to display sql digest corresponding sql and execution plan related information where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			switch {
			case a.startTime != "" && a.endTime == "":
				return fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
			case a.startTime == "" && a.endTime != "":
				return fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
			case a.startTime != "" && a.endTime != "":
				// reset nearly 30 minutes options, --start and --end have higher priority
				a.nearly = 0
			}
			if a.sqlDigest == "" {
				return fmt.Errorf(`the sql_digest cannot be empty, required flag(s) --sql-digest {sqlDigest} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(sql.NewSqlQueryModel(
				a.clusterName,
				a.nearly,
				a.enableHistory,
				a.startTime,
				a.endTime,
				"DISPLAY",
				"",
				a.sqlDigest,
				a.trend,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(sql.SqlQueryModel)
			if lModel.Error != nil {
				return lModel.Error
			}
			if lModel.Msgs != nil {
				resp := lModel.Msgs.(*sql.QueriedRespMsg)
				if reflect.DeepEqual(resp, &sql.QueriedRespMsg{}) {
					fmt.Println("the cluster sql display not found, please ignore and skip")
					return nil
				}

				sql.PrintSqlDisplaySummaryComment(a.sqlDigest)
				if err := model.QueryResultFormatTableStyleWithRowsArray(resp.QueriedSummary.Columns, resp.QueriedSummary.Results); err != nil {
					return err
				}
				sql.PrintSqlDisplayTrendSummaryComment(a.trend)
				if err := model.QueryResultFormatTableStyleWithRowsArray(resp.QueriedTrendSummary.Columns, resp.QueriedTrendSummary.Results); err != nil {
					return err
				}
				sql.PrintSqlDisplayPlanSummaryComment()
				if err := model.QueryResultFormatTableStyleWithRowsArray(resp.QueriedPlanSummary.Columns, resp.QueriedPlanSummary.Results); err != nil {
					return err
				}

				for _, p := range resp.QueriedPlanDetail {
					fmt.Printf("<<<<<<<<<<<< USERNAME [%s] SCHEMA_NAME [%s] >>>>>>>>>>>>\n", p.SampleUser, p.SchemaName)
					fmt.Printf("MIN PLAN: %s\n", p.MinPlan.PlanDigest)
					fmt.Println(p.MinPlan.SqlText + "\n")
					fmt.Println(p.MinPlan.SqlPlan)
					fmt.Printf("\n------\n")
					fmt.Printf("MAX PLAN: %s\n", p.MaxPlan.PlanDigest)
					fmt.Println(p.MaxPlan.SqlText + "\n")
					fmt.Println()
					fmt.Println(p.MaxPlan.SqlPlan + "\n")
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().IntVar(&a.trend, "trend", 3, "configure the number of sql digest samples, that is, set the statements_summary refresh interval to 1 sampling point")
	return cmd
}

type AppSqlBinding struct {
	*AppSql
	schemaName string
}

func (a *AppSql) AppSqlBinding() Cmder {
	return &AppSqlBinding{AppSql: a}
}

func (a *AppSqlBinding) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bind",
		Short: "Bind used to perform global binding of SQL statement execution plan based on SQL digest",
		Long:  "Bind used to perform global binding of SQL statement execution plan based on SQL digest where the specified cluster name is located",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.Help(); err != nil {
				return err
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.PersistentFlags().StringVar(&a.schemaName, "schema", "", "configure the schema_name where the sql digest fingerprint is located. If the schema_name value is *, it means cross join sql digest")
	return cmd
}

type AppSqlBindingCreate struct {
	*AppSqlBinding
}

func (a *AppSqlBinding) AppSqlBindingCreate() Cmder {
	return &AppSqlBindingCreate{AppSqlBinding: a}
}

func (a *AppSqlBindingCreate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create used to create global binding of SQL statement execution plan based on SQL digest",
		Long:  "Create used to create global binding of SQL statement execution plan based on SQL digest where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			switch {
			case a.startTime != "" && a.endTime == "":
				return fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
			case a.startTime == "" && a.endTime != "":
				return fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
			case a.startTime != "" && a.endTime != "":
				// reset nearly 30 minutes options, --start and --end have higher priority
				a.nearly = 0
			}
			if a.sqlDigest == "" {
				return fmt.Errorf(`the sql_digest cannot be empty, required flag(s) --sql-digest {sqlDigest} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(sql.NewSqlBindCreateModel(
				a.clusterName,
				a.nearly,
				a.enableHistory,
				a.startTime,
				a.endTime,
				a.schemaName,
				a.sqlDigest,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(sql.SqlBindCreateModel)
			if lModel.Error != nil {
				return lModel.Error
			}
			if lModel.Msgs != nil {
				fmt.Println(lModel.Msgs.(string))
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}

type AppSqlBindingQuery struct {
	*AppSqlBinding
}

func (a *AppSqlBinding) AppSqlBindingQuery() Cmder {
	return &AppSqlBindingQuery{AppSqlBinding: a}
}

func (a *AppSqlBindingQuery) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query used to query metadata global binding of SQL statement execution plan based on SQL digest",
		Long:  "Query used to query metadata global binding of SQL statement execution plan based on SQL digest where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(sql.NewSqlQueryModel(
				a.clusterName,
				a.nearly,
				a.enableHistory,
				a.startTime,
				a.endTime,
				"QUERY",
				a.schemaName,
				a.sqlDigest,
				0,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(sql.SqlQueryModel)
			if lModel.Error != nil {
				return lModel.Error
			}
			if lModel.Msgs != nil {
				switch val := lModel.Msgs.(type) {
				case *sql.QueriedResultMsg:
					if reflect.DeepEqual(val, &sql.QueriedResultMsg{}) {
						fmt.Println("the cluster sql binding metadata not found, please ignore and skip")
						return nil
					}
					fmt.Println("the cluster sql binding queried records:")
					if err := model.QueryResultFormatTableStyleWithRowsArray(val.Columns, val.Results); err != nil {
						return err
					}
					fmt.Println("Determine whether the database sql binding actually exists, please see by [select * from mysql.bind_info order by create_time desc limit 5].")
				case string:
					fmt.Println(val)
				default:
					return fmt.Errorf("unknown model msg type [%s]", val)
				}

			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}

type AppSqlBindingDelete struct {
	*AppSqlBinding
}

func (a *AppSqlBinding) AppSqlBindingDelete() Cmder {
	return &AppSqlBindingDelete{AppSqlBinding: a}
}

func (a *AppSqlBindingDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete used to delete global binding of SQL statement execution plan based on SQL digest",
		Long:  "Delete used to delete global binding of SQL statement execution plan based on SQL digest where the specified cluster name is located",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.clusterName == "" {
				return fmt.Errorf(`the cluster_name cannot be empty, required flag(s) -c {clusterName} not set`)
			}
			if a.sqlDigest == "" {
				return fmt.Errorf(`the sql_digest cannot be empty, required flag(s) --sql-digest {sqlDigest} not set`)
			}
			if a.schemaName == "" {
				return fmt.Errorf(`the schema cannot be empty, required flag(s) --schema {schemaName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			p := tea.NewProgram(sql.NewSqlQueryModel(
				a.clusterName,
				a.nearly,
				a.enableHistory,
				a.startTime,
				a.endTime,
				"DELETE",
				a.schemaName,
				a.sqlDigest,
				0,
			))
			teaModel, err := p.Run()
			if err != nil {
				return err
			}
			lModel := teaModel.(sql.SqlQueryModel)
			if lModel.Error != nil {
				return lModel.Error
			}
			if lModel.Msgs != nil {
				switch val := lModel.Msgs.(type) {
				case *sql.QueriedResultMsg:
					if reflect.DeepEqual(val, &sql.QueriedResultMsg{}) {
						fmt.Println("the cluster sql binding records not found, please ignore and skip")
						return nil
					}

					fmt.Println("the cluster sql binding deleted records:")
					if err := model.QueryResultFormatTableStyleWithRowsArray(val.Columns, val.Results); err != nil {
						return err
					}
					fmt.Println("Determine whether the database sql binding has been deleted, please see by [select * from mysql.bind_info order by create_time desc limit 5].")
				case string:
					fmt.Println(val)
				default:
					return fmt.Errorf("unknown model msg type [%s]", val)
				}
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}
