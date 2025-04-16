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
package model

import (
	"fmt"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
)

func QueryResultFormatTableStyle(columns []string, results []map[string]string, sec ...float64) error {
	t := table.NewWriter()

	var header table.Row
	for _, c := range columns {
		header = append(header, c)
	}
	t.AppendHeader(header)
	t.AppendSeparator()
	t.AppendRows(queryResultProcess(columns, results))
	if len(sec) > 0 {
		t.SetCaption("%d rows in set (%.2f sec)\n", len(results), sec[0])
		_, err := fmt.Fprintln(os.Stdout, t.Render())
		if err != nil {
			return err
		}
		return nil
	}
	_, err := fmt.Fprintln(os.Stdout, t.Render()+"\n")
	if err != nil {
		return err
	}
	return nil
}

func QueryResultFormatWithoutTableStyle(columns []string, results []map[string]string, sec ...float64) error {
	var bs []string

	for i, res := range results {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("*************************** %d. row ***************************\n", i+1))
		for _, c := range columns {
			for k, v := range res {
				if k == c {
					if v == "NULLABLE" {
						b.WriteString(fmt.Sprintf("  %s: %v\n", c, "NULL"))
					} else {
						b.WriteString(fmt.Sprintf("  %s: %v\n", c, v))
					}
				}
			}
		}
		bs = append(bs, b.String())
	}
	bs = append(bs, fmt.Sprintf("%d rows in set (%.2f sec)\n", len(results), sec[0]))
	_, err := fmt.Fprintln(os.Stdout, strings.Join(bs, "\n"))
	if err != nil {
		return err
	}
	return nil
}

func queryResultProcess(columns []string, results []map[string]string) []table.Row {
	var rows []table.Row

	for _, res := range results {
		var newRow table.Row
		for _, c := range columns {
			for k, v := range res {
				if k == c {
					if v == "NULLABLE" {
						newRow = append(newRow, "NULL")
					} else {
						newRow = append(newRow, v)
					}
				}
			}
		}
		rows = append(rows, newRow)
	}
	return rows
}

func QueryResultFormatTableStyleWithRowsArray(columns []string, rows [][]interface{}) error {
	t := table.NewWriter()

	var header table.Row
	for _, c := range columns {
		header = append(header, c)
	}
	t.AppendHeader(header)
	t.AppendSeparator()
	var newRows []table.Row
	for _, row := range rows {
		var newRow table.Row
		for _, r := range row[:len(columns)] {
			newRow = append(newRow, r)
		}
		newRows = append(newRows, newRow)
	}

	t.AppendRows(newRows)

	_, err := fmt.Fprintln(os.Stdout, t.Render()+"\n")
	if err != nil {
		return err
	}
	return nil
}
