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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
)

func GenerateQueryWindowSqlElapsedTime(nearly int, start, end string, enableHistory bool) (string, error) {
	var bs strings.Builder
	bs.WriteString(`select 
     SUM(sum_latency) / 1000000000 as "all_latency_s"` + "\n")

	if enableHistory {
		bs.WriteString(`FROM information_schema.cluster_statements_summary_history a` + "\n")
	} else {
		bs.WriteString(`FROM information_schema.cluster_statements_summary a` + "\n")
	}
	if nearly > 0 {
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= NOW()
    AND summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, nearly))
	} else {
		if start == "" || end == "" {
			return "", fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
		}
		bs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= '%s'
    AND summary_end_time >= '%s'
    AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, end, start))
	}

	return bs.String(), nil
}

func GenerateSqlDisplaySummaryQuery(nearly int, start, end string, enableHistory bool, totalLatency string, sqlDigest string) (string, error) {
	var bs strings.Builder
	bs.WriteString(fmt.Sprintf(`/*+ monitoring */ SELECT
	SAMPLE_USER as "sample_user",
	SCHEMA_NAME as "schema_name",
    SUM(sum_latency) / 1000000000 as "total_latency_s",
    SUM(exec_count) as total_execs,
    AVG(avg_latency) / 1000000000 "avg_latency_s",
    AVG(avg_parse_latency) / 1000000000 "avg_parse_latency_s",
    AVG(avg_compile_latency) / 1000000000 "avg_compile_latency_s",
    AVG(AVG_TOTAL_KEYS) as avg_total_keys,
    AVG(AVG_PROCESSED_KEYS) as avg_processed_keys,
    ROUND(IFNULL(SUM(sum_latency),0) / 1000000000 / %v,2) "percentage"`, totalLatency) + "\n")
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
	bs.WriteString(fmt.Sprintf("AND a.digest = '%s'\n", sqlDigest))
	bs.WriteString(`GROUP BY
    SAMPLE_USER,
    DIGEST,
    SCHEMA_NAME`)

	return bs.String(), nil
}

func SqlDisplayQuery(ctx context.Context, clusterName string, nearly int, start, end string, enableHistory bool, sqlDigest string, trend int) (*QueriedRespMsg, error) {
	qrsm := &QueriedRespMsg{}

	connDB, err := database.Connector.GetDatabase(clusterName)
	if err != nil {
		return nil, err
	}
	db := connDB.(*mysql.Database)

	totalLatencySql, err := GenerateQueryWindowSqlElapsedTime(nearly, start, end, enableHistory)
	if err != nil {
		return nil, err
	}

	_, res, err := db.GeneralQuery(ctx, totalLatencySql)
	if err != nil {
		return nil, err
	}
	var totalLatency string
	if len(res) == 0 {
		return nil, fmt.Errorf("the database sql [%v] query time windows result not found", totalLatencySql)
	} else {
		if res[0]["all_latency_s"] == "NULLABLE" {
			totalLatency = "1"
		} else {
			totalLatency = res[0]["all_latency_s"]
		}
	}

	disummarieSql, err := GenerateSqlDisplaySummaryQuery(nearly, start, end, enableHistory, totalLatency, sqlDigest)
	if err != nil {
		return nil, err
	}
	_, res, err = db.GeneralQuery(ctx, disummarieSql)
	if err != nil {
		return nil, err
	}

	var (
		rows [][]interface{}
	)
	sampleSchemaUniqs := make(map[string]struct{})
	for _, r := range res {
		var row []interface{}

		sampleSchemaUniqs[fmt.Sprintf("%s.%s", r["sample_user"], r["schema_name"])] = struct{}{}

		row = append(row, fmt.Sprintf("%s[%s]", r["sample_user"], r["schema_name"]))
		row = append(row, r["total_latency_s"])
		row = append(row, r["total_execs"])
		row = append(row, r["avg_latency_s"])
		row = append(row, r["avg_parse_latency_s"])
		row = append(row, r["avg_compile_latency_s"])
		row = append(row, r["avg_total_keys"])
		row = append(row, r["avg_processed_keys"])

		// total sql time percent
		float, err := decimal.NewFromString(r["percentage"])
		if err != nil {
			return nil, err
		}
		row = append(row, fmt.Sprintf("%v%%", float.Mul(decimal.NewFromInt(100))))

		rows = append(rows, row)
	}
	qrsm.QueriedSummary = &QueriedResultMsg{
		Columns: []string{"Username[Schema]", "Elapsed(s)", "Executions", "Latency Per Exec(s)", "Latency Per Parse(s)", "Latency Per Compile(s)", "Avg Total Keys", "Avg Processed Keys", `% Total SQL Time`},
		Results: rows,
	}

	var plans [][]interface{}
	var planDetails []*QueiredPlanMsg

	for str, _ := range sampleSchemaUniqs {
		sli := strings.Split(str, ".")
		sampleUser := sli[0]
		schemaName := sli[1]

		var plands strings.Builder
		plands.WriteString(`SELECT
			sub.schema_name,
			sub.sample_user,
			sub.plan_digest,
			ROUND(IFNULL(SUM(sub.sum_latency),0) / 1000000000 ,4) as sum_latency,
			sum(sub.exec_count) as exec_counts,
			ROUND(IFNULL(avg(sub.avg_latency),0) / 1000000000 ,4) as avg_latency,
			AVG(sub.AVG_TOTAL_KEYS) as avg_total_keys,
    		AVG(sub.AVG_PROCESSED_KEYS) as avg_processed_keys,
			MIN(sub.plan) as sql_plan,
    		MIN(sub.query_sample_text) as sql_text` + "\n")
		if enableHistory {
			plands.WriteString(`FROM information_schema.cluster_statements_summary_history sub` + "\n")
		} else {
			plands.WriteString(`FROM information_schema.cluster_statements_summary sub` + "\n")
		}
		if nearly > 0 {
			plands.WriteString(fmt.Sprintf(`WHERE sub.summary_begin_time <= NOW()
		AND sub.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
		AND sub.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, nearly) + "\n")
		} else {
			if start == "" || end == "" {
				return nil, fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
			}
			plands.WriteString(fmt.Sprintf(`WHERE sub.summary_begin_time <= '%s'
		AND sub.summary_end_time >= '%s'
		AND sub.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, end, start) + "\n")
		}
		plands.WriteString(fmt.Sprintf("AND sub.digest = '%s' AND sub.sample_user = '%s'", sqlDigest, sampleUser))
		if schemaName == "NULLABLE" {
			plands.WriteString(" AND sub.schema_name IS NULL GROUP BY sub.schema_name,sub.sample_user,sub.plan_digest")
		} else {
			plands.WriteString(fmt.Sprintf(" AND sub.schema_name = '%s' GROUP BY sub.schema_name,sub.sample_user,sub.plan_digest", schemaName))
		}
		_, res, err := db.GeneralQuery(ctx, plands.String())
		if err != nil {
			return nil, err
		}

		for _, r := range res {
			var p []interface{}
			if r["schema_name"] == "NULLABLE" {
				r["schema_name"] = "NULL"
			}
			p = append(p, fmt.Sprintf("%s[%s]", r["sample_user"], r["schema_name"]))
			p = append(p, r["plan_digest"])
			p = append(p, r["sum_latency"])
			p = append(p, r["exec_counts"])
			p = append(p, r["avg_latency"])
			p = append(p, r["avg_total_keys"])
			p = append(p, r["avg_processed_keys"])
			plans = append(plans, p)

			planDetail := &QueiredPlanMsg{}

			planDetail.SampleUser = sampleUser
			if schemaName == "NULLABLE" {
				planDetail.SchemaName = "NULL"
			} else {
				planDetail.SchemaName = schemaName
			}
			planDetail.PlanDigest = r["plan_digest"]
			avgLat, err := strconv.ParseFloat(r["avg_latency"], 64)
			if err != nil {
				return nil, err
			}
			planDetail.AvgLatency = avgLat
			planDetail.SqlText = r["sql_text"]
			planDetail.SqlPlan = r["sql_plan"]
			planDetails = append(planDetails, planDetail)
		}
	}

	qrsm.QueriedPlanSummary = &QueriedResultMsg{
		Columns: []string{"Username[Schema]", "Plan Digest", "Total Latency(s)", "Executions", "Avg Latency(s)", "Avg Total Keys", "Avg Processed Keys"},
		Results: plans,
	}

	sort.Slice(planDetails, func(i, j int) bool {
		return planDetails[i].AvgLatency <= planDetails[j].AvgLatency
	})

	qrsm.QueriedPlanDetail = planDetails

	// quried trend
	_, res, err = db.GeneralQuery(ctx, `select max(summary_begin_time) AS s_begin from information_schema.statements_summary_history`)
	if err != nil {
		return qrsm, err
	}

	layout := "2006-01-02 15:04:05"

	parsedTime, err := time.ParseInLocation(layout, res[0]["s_begin"], time.Local)
	if err != nil {
		return qrsm, fmt.Errorf("failed to parse time: %w", err)
	}

	_, res, err = db.GeneralQuery(ctx, `show variables like 'tidb_stmt_summary_refresh_interval'`)
	if err != nil {
		return qrsm, err
	}

	refreshSec, err := strconv.Atoi(res[0]["Value"])
	if err != nil {
		return qrsm, err
	}

	var trends [][]string

	var ts []string
	beginTime := parsedTime.Add(-time.Duration(refreshSec) * time.Second)
	ts = append(ts, beginTime.Format(layout))
	ts = append(ts, parsedTime.Format(layout))

	trends = append(trends, ts)

	for i := 2; i <= trend; i++ {
		var ts []string
		beginTime := parsedTime.Add(-time.Duration(refreshSec*i) * time.Second)
		endTime := parsedTime.Add(-time.Duration(refreshSec*(i-1)) * time.Second)
		ts = append(ts, beginTime.Format(layout))
		ts = append(ts, endTime.Format(layout))
		trends = append(trends, ts)
	}

	var (
		histRows [][]interface{}
	)

	for _, ts := range trends {
		allLatencySql, err := GenerateQueryWindowSqlElapsedTime(0, ts[0], ts[1], true)
		if err != nil {
			return qrsm, err
		}

		_, res, err := db.GeneralQuery(ctx, allLatencySql)
		if err != nil {
			return nil, err
		}
		var totalLatency string
		if len(res) == 0 {
			return nil, fmt.Errorf("the database history sql [%v] query time windows result not found", allLatencySql)
		} else {
			if res[0]["all_latency_s"] == "NULLABLE" {
				totalLatency = "1"
			} else {
				totalLatency = res[0]["all_latency_s"]
			}
		}

		disummarieSql, err := GenerateSqlDisplaySummaryQuery(0, ts[0], ts[1], true, totalLatency, sqlDigest)
		if err != nil {
			return nil, err
		}
		_, res, err = db.GeneralQuery(ctx, disummarieSql)
		if err != nil {
			return nil, err
		}

		for _, r := range res {
			var row []interface{}
			row = append(row, fmt.Sprintf("%s / %s", ts[0], ts[1]))
			row = append(row, fmt.Sprintf("%s[%s]", r["sample_user"], r["schema_name"]))
			row = append(row, r["total_latency_s"])
			row = append(row, r["total_execs"])
			row = append(row, r["avg_latency_s"])
			row = append(row, r["avg_parse_latency_s"])
			row = append(row, r["avg_compile_latency_s"])
			row = append(row, r["avg_total_keys"])
			row = append(row, r["avg_processed_keys"])

			// total sql time percent
			float, err := decimal.NewFromString(r["percentage"])
			if err != nil {
				return nil, err
			}
			row = append(row, fmt.Sprintf("%v%%", float.Mul(decimal.NewFromInt(100))))
			histRows = append(histRows, row)
		}
	}

	qrsm.QueriedTrendSummary = &QueriedResultMsg{
		Columns: []string{"Time Range", "Username[Schema]", "Elapsed(s)", "Executions", "Latency Per Exec(s)", "Latency Per Parse(s)", "Latency Per Compile(s)", "Avg Total Keys", "Avg Processed Keys", `% Total SQL Time`},
		Results: histRows,
	}
	return qrsm, nil
}

func PrintSqlDisplaySummaryComment(sqlDigest string) {
	fmt.Printf("记录时间窗口内 SQL 指纹信息概览: %s\n", sqlDigest)
	fmt.Println("NOTES：")
	fmt.Println("- 默认以当前 statement_summary 数据表查询，如需使用 history 表，请使用 --enable-history")
	fmt.Println("- SQL 文本默认隐藏不显示，如需显示请使用 --enable-sql，且 ONLY 显示执行计划耗时最小以及最大的 SQL 文本]")
	fmt.Println(`
Username[Schema]：SQL 对应业务用户名以及所在 Schema 名
Elapsed(s)： SQL 执行总耗时
Executions：SQL 执行总次数
Latency Per Exec(s)：SQL 平均每次执行耗时
Latency Per Parse(s)：SQL 平均每次解析耗时
Latency Per Compile(s)：SQL 平均每次编译耗时
Avg total keys：Coprocessor 扫过的 key 的平均数量
Avg processed keys：Coprocessor 处理的 key 的平均数量。相比 avg_total_keys，avg_processed_keys 不包含 MVCC 的旧版本。如果 avg_total_keys 和 avg_processed_keys 相差很大，说明旧版本比较多
% Total SQL Time：SQL 耗时占时间窗口内所有 SQL 耗时百分比`)
}

func PrintSqlDisplayTrendSummaryComment(trend int) {
	fmt.Println(`NOTES:`)
	fmt.Printf("- 当前时间窗口非固定以 statement_summary refresh 周期为时间段查询(看查询条件)，而以 refresh 周期为时间段查询过往同个 SQL 执行情况（同比），获取 %d 个时间段内对应 SQL 指纹执行信息\n", trend)
}

func PrintSqlDisplayPlanSummaryComment() {
	fmt.Println(`NOTES:`)
	fmt.Printf("- 查看时间窗口内 SQL 指纹执行计划概览信息，并显示 SQL 指纹相关执行计划所用耗时\n")
	fmt.Printf("- SQL 指纹平均耗时对应最小以及最大的执行详情默认不显示，如需显示请使用 --enable-sql 参数运行\n")
	fmt.Println(`
Username[Schema]：SQL 对应业务用户名以及所在 Schema 名
Plan Digest： SQL 对应执行计划指纹
Total Latency(s)： SQL 执行计划指纹对应总耗时
Executions： SQL 执行计划指纹对应执行次数
Avg Latency(s)： SQL 执行计划指纹对应平均执行耗时
Avg total keys：Coprocessor 扫过的 key 的平均数量
Avg processed keys：Coprocessor 处理的 key 的平均数量。相比 avg_total_keys，avg_processed_keys 不包含 MVCC 的旧版本。如果 avg_total_keys 和 avg_processed_keys 相差很大，说明旧版本比较多`)
}
