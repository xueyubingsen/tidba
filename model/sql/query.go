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
    ROUND(SUM(sum_latency) / 1000000000 / %v,2) "percentage"`, totalLatency) + "\n")
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
		totalLatency = res[0]["all_latency_s"]
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
		planDetail := &QueiredPlanMsg{}

		sli := strings.Split(str, ".")
		sampleUser := sli[0]
		schemaName := sli[1]

		planDetail.SampleUser = sampleUser
		planDetail.SchemaName = schemaName

		var minPlanbs strings.Builder
		minPlanbs.WriteString(`SELECT
			plan_digest,
			query_sample_text,
			plan` + "\n")
		if enableHistory {
			minPlanbs.WriteString(`FROM information_schema.cluster_statements_summary_history sub_min` + "\n")
		} else {
			minPlanbs.WriteString(`FROM information_schema.cluster_statements_summary sub_min` + "\n")
		}
		if nearly > 0 {
			minPlanbs.WriteString(fmt.Sprintf(`WHERE sub_min.summary_begin_time <= NOW()
		AND sub_min.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
		AND sub_min.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, nearly) + "\n")
		} else {
			if start == "" || end == "" {
				return nil, fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
			}
			minPlanbs.WriteString(fmt.Sprintf(`WHERE sub_min.summary_begin_time <= '%s'
		AND sub_min.summary_end_time >= '%s'
		AND sub_min.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, end, start) + "\n")
		}
		minPlanbs.WriteString(fmt.Sprintf("AND sub_min.digest = '%s' AND sub_min.sample_user = '%s' AND sub_min.schema_name = '%s' ORDER BY sub_min.min_latency ASC LIMIT 1", sqlDigest, sampleUser, schemaName))

		_, res, err = db.GeneralQuery(ctx, minPlanbs.String())
		if err != nil {
			return nil, err
		}

		minPlanDigest := res[0]["plan_digest"]

		planDetail.MinPlan = &QueriedPlan{
			PlanDigest: minPlanDigest,
			SqlText:    res[0]["query_sample_text"],
			SqlPlan:    res[0]["plan"],
		}

		var maxPlanbs strings.Builder
		maxPlanbs.WriteString(`SELECT
			plan_digest,
			query_sample_text,
			plan` + "\n")
		if enableHistory {
			maxPlanbs.WriteString(`FROM information_schema.cluster_statements_summary_history sub_max` + "\n")
		} else {
			maxPlanbs.WriteString(`FROM information_schema.cluster_statements_summary sub_max` + "\n")
		}
		if nearly > 0 {
			maxPlanbs.WriteString(fmt.Sprintf(`WHERE sub_max.summary_begin_time <= NOW()
		AND sub_max.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
		AND sub_max.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, nearly) + "\n")
		} else {
			if start == "" || end == "" {
				return nil, fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
			}
			maxPlanbs.WriteString(fmt.Sprintf(`WHERE sub_max.summary_begin_time <= '%s'
		AND sub_max.summary_end_time >= '%s'
		AND sub_max.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, end, start) + "\n")
		}
		maxPlanbs.WriteString(fmt.Sprintf("AND sub_max.digest = '%s' AND sub_max.sample_user = '%s' AND sub_max.schema_name = '%s' ORDER BY sub_max.max_latency DESC LIMIT 1", sqlDigest, sampleUser, schemaName))

		_, res, err = db.GeneralQuery(ctx, maxPlanbs.String())
		if err != nil {
			return nil, err
		}

		maxPlanDigest := res[0]["plan_digest"]

		planDetail.MaxPlan = &QueriedPlan{
			PlanDigest: maxPlanDigest,
			SqlText:    res[0]["query_sample_text"],
			SqlPlan:    res[0]["plan"],
		}

		var newBs strings.Builder
		newBs.WriteString(`/*+ monitoring */ SELECT
		sample_user,
		schema_name,
		COUNT(DISTINCT plan_digest) AS plan_digests,
		MIN(min_latency) / 1000000000 AS min_latency_s,
		MAX(max_latency) / 1000000000 AS max_latency_s` + "\n")
		if enableHistory {
			newBs.WriteString(`FROM information_schema.cluster_statements_summary_history a` + "\n")
		} else {
			newBs.WriteString(`FROM information_schema.cluster_statements_summary a` + "\n")
		}
		if nearly > 0 {
			newBs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= NOW()
		AND a.summary_end_time >= DATE_ADD(NOW(), INTERVAL - %d MINUTE)
		AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, nearly) + "\n")
		} else {
			if start == "" || end == "" {
				return nil, fmt.Errorf("to avoid the query range being too large, you need to explicitly set the flag [--start] and flag [--end] query range")
			}
			newBs.WriteString(fmt.Sprintf(`WHERE a.summary_begin_time <= '%s'
		AND a.summary_end_time >= '%s'
		AND a.QUERY_SAMPLE_TEXT NOT LIKE '%%/*+ monitoring */%%'`, end, start) + "\n")
		}
		newBs.WriteString(fmt.Sprintf("AND a.digest = '%s' AND a.sample_user = '%s' AND a.schema_name = '%s'\n", sqlDigest, sampleUser, schemaName))
		newBs.WriteString(`GROUP BY
		SAMPLE_USER,
		DIGEST,
		SCHEMA_NAME`)

		_, res, err = db.GeneralQuery(ctx, newBs.String())
		if err != nil {
			return nil, err
		}

		for _, r := range res {
			var p []interface{}
			p = append(p, fmt.Sprintf("%s[%s]", r["sample_user"], r["schema_name"]))
			p = append(p, r["plan_digests"])
			p = append(p, fmt.Sprintf("%s[%s]", r["min_latency_s"], minPlanDigest))
			p = append(p, fmt.Sprintf("%s[%s]", r["max_latency_s"], maxPlanDigest))
			plans = append(plans, p)
		}

		planDetails = append(planDetails, planDetail)
	}

	qrsm.QueriedPlanSummary = &QueriedResultMsg{
		Columns: []string{"Username[Schema]", "Plan Digests", "Min Plan Digest Latency(s)", "Max Plan Digest Latency(s)"},
		Results: plans,
	}
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
			totalLatency = res[0]["all_latency_s"]
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
	fmt.Printf("- 当前时间窗口非固定以 statement_summary refresh 周期为时间段查询(看查询条件)，而查询过往同个 SQL 执行情况以 refresh 周期为时间段，获取 %d 个时间段内对应 SQL 指纹执行信息\n", trend)
}

func PrintSqlDisplayPlanSummaryComment() {
	fmt.Println(`NOTES:`)
	fmt.Printf("- 查看时间窗口内 SQL 指纹执行计划概览信息，并显示最小以及最大执行计划所用耗时")
	fmt.Println(`
Username[Schema]：SQL 对应业务用户名以及所在 Schema 名
Plan Digests： SQL 对应执行计划数
Min Plan Digest Latency(s)： SQL 最小执行计划 Digest 所用耗时
Max Plan Digest Latency(s)： SQL 最大执行计划 Digest 所用耗时`)
}
