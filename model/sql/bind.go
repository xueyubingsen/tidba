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
	"strings"

	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/database/sqlite"
)

func SqlQueryCreateBind(ctx context.Context, clusterName string, nearly int, start, end string, enableHistory bool, schemaName string, sqlDigest string, sqlBindingText string) error {
	connDB, err := database.Connector.GetDatabase(clusterName)
	if err != nil {
		return err
	}
	db := connDB.(*mysql.Database)

	metaDB, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
	if err != nil {
		return err
	}
	meta := metaDB.(*sqlite.Database)

	query, err := GenerateSqlQueryDigest(nearly, start, end, enableHistory, schemaName, sqlDigest)
	if err != nil {
		return err
	}

	_, sqlInfos, err := db.GeneralQuery(ctx, query)
	if err != nil {
		return err
	}

	for _, r := range sqlInfos {
		var (
			schemaName string
			bindsqls   []string
		)
		digestText := r["digest_text"]

		// cross schema join
		if r["schema_name"] == "NULLABLE" {
			bindsqls = append(bindsqls, fmt.Sprintf("CREATE GLOBAL BINDING FOR %s USING %s", digestText, sqlBindingText))
			schemaName = "*"
		} else {
			bindsqls = append(bindsqls, fmt.Sprintf("USE %s", r["schema_name"]))
			bindsqls = append(bindsqls, fmt.Sprintf("CREATE GLOBAL BINDING FOR %s USING %s", digestText, sqlBindingText))
			schemaName = r["schema_name"]
		}
		for _, s := range bindsqls {
			if _, err := db.ExecContext(ctx, s); err != nil {
				return err
			}
		}
		if _, err := meta.CreateSqlBinding(ctx, &sqlite.SqlBinding{
			ClusterName:  clusterName,
			SchemaName:   schemaName,
			SqlDigest:    r["sql_digest"],
			DigestText:   digestText,
			OptimizeText: sqlBindingText,
		}); err != nil {
			return err
		}
	}
	return nil
}

func GenerateSqlQueryDigest(nearly int, start, end string, enableHistory bool, schemaName string, sqlDigest string) (string, error) {
	var bs strings.Builder
	bs.WriteString(`/*+ monitoring */ SELECT
	SCHEMA_NAME as "schema_name",
	digest as "sql_digest",
    min(digest_text) as "digest_text"` + "\n")
	if enableHistory {
		bs.WriteString(`FROM information_schema.cluster_statements_summary_history a` + "\n")
	} else {
		bs.WriteString(`FROM information_schema.cluster_statements_summary a` + "\n")
	}
	if nearly > 0 {
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= NOW()
    AND a.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, nearly) + "\n")
	} else {
		if start == "" || end == "" {
			return "", fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
		}
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= '%s'
    AND a.summary_end_time >= '%s'
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, end, start) + "\n")
	}
	if schemaName != "" && !strings.EqualFold(schemaName, "*") {
		bs.WriteString(fmt.Sprintf("AND a.schema_name = '%s' AND a.digest = '%s'\n", schemaName, sqlDigest))
	} else if schemaName != "" && strings.EqualFold(schemaName, "*") {
		bs.WriteString(fmt.Sprintf("AND a.schema_name is null AND a.digest = '%s'\n", sqlDigest))
	} else {
		bs.WriteString(fmt.Sprintf("AND a.digest = '%s' AND a.schema_name not in ('performance_schema','mysql','information_schema','metrics_schema')\n", sqlDigest))
	}
	bs.WriteString(`GROUP BY
    DIGEST,
    SCHEMA_NAME`)
	return bs.String(), nil
}
