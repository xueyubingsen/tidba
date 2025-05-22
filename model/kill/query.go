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
package kill

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/logger"
)

func GenerateKillSessionSqlBySqlDigest(ctx context.Context, clusterName string, sqlDigests []string, duration, interval, concurrency int) error {
	connDB, err := database.Connector.GetDatabase(clusterName)
	if err != nil {
		return err
	}
	db := connDB.(*mysql.Database)

	_, res, err := db.GeneralQuery(ctx, "show config where `type`='tidb' and name ='enable-global-kill'")
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("the cluster name [%v] database version not meet requirement, require version >= v6.1.0 and config [enable-global-kill = true]", clusterName)
	}

	var digests []string
	for _, s := range sqlDigests {
		digests = append(digests, fmt.Sprintf("'%s'", s))
	}

	queryStr := fmt.Sprintf(`SELECT
	concat_ws(':',f.instance,t.ID) AS inst
FROM
	information_schema.cluster_processlist t
LEFT JOIN information_schema.cluster_info f ON
	t.INSTANCE = f.STATUS_ADDRESS
WHERE
	t.digest IN (%s)`, strings.Join(digests, ","))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if duration > 0 {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(ctx, time.Duration(duration)*time.Second)
		defer cancelFunc()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM) // 捕获 Ctrl+C 信号

	round := 0

	var sessionCounts atomic.Uint64

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				utime := time.Now()
				_, results, err := db.GeneralQuery(ctx, queryStr)
				if err != nil {
					logger.Error("query error", zap.String("query", queryStr), zap.Error(err))
					cancel()
					return
				}
				counts := len(results)
				logger.Info(fmt.Sprintf("started round [%d] kill sql digests session operation...", round))
				logger.Info(fmt.Sprintf("killed sql digests session generate list finished in %fs, session counts [%d] ", time.Since(utime).Seconds(), counts))

				if counts == 0 {
					logger.Info(fmt.Sprintf("completed round [%d] kill sql digests session counts [%d] operation finished in %fs", round, sessionCounts.Load(), time.Since(utime).Seconds()))
					time.Sleep(time.Duration(interval) * time.Millisecond)
					round++
					continue
				}

				sessionCounts.Store(uint64(counts))

				g, gCtx := errgroup.WithContext(ctx)
				g.SetLimit(concurrency)
				for _, res := range results {
					r := res
					g.Go(func() error {
						stime := time.Now()
						instS := strings.Split(r["inst"], ":")
						if _, err := db.ExecContext(gCtx, fmt.Sprintf("kill tidb %s", instS[2])); err != nil {
							return err
						}
						// -1 operation
						sessionCounts.Add(^uint64(0))
						logger.Info(fmt.Sprintf("killed sql digests session on [%s:%s] with id [%s] finished in %fs, session counts [%d]", instS[0], instS[1], instS[2], time.Since(stime).Seconds(), sessionCounts.Load()))
						return nil
					})
				}
				if err := g.Wait(); err != nil {
					logger.Error("kill error", zap.Error(err))
					cancel()
					return
				}

				logger.Info(fmt.Sprintf("completed round [%d] kill sql digests session counts [%d] operation finished in %fs", round, sessionCounts.Load(), time.Since(utime).Seconds()))
				time.Sleep(time.Duration(interval) * time.Millisecond)
				round++
			}
		}
	}(ctx)

	select {
	case <-sigChan:
		cancel()
		logger.Error("signal error", zap.Error(errors.New("receive Ctrl+C signal, interrupt program execution")))
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			logger.Error("timeout error", zap.Error(fmt.Errorf("the running time has expired [--duration %d] and the program automatically exited", duration)))
		} else {
			logger.Error("cancel error", zap.Error(errors.New("the program was canceled by error cancel, the program ends automatically")))
		}
	}
	return nil
}

func GenerateKillSessionSqlByUsername(ctx context.Context, clusterName string, usernames []string, duration, interval, concurrency int) error {
	connDB, err := database.Connector.GetDatabase(clusterName)
	if err != nil {
		return err
	}
	db := connDB.(*mysql.Database)

	_, res, err := db.GeneralQuery(ctx, "show config where `type`='tidb' and name ='enable-global-kill'")
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("the cluster name [%v] database version not meet requirement, require version >= v6.1.0 and config [enable-global-kill = true]", clusterName)
	}

	var users []string
	for _, s := range usernames {
		users = append(users, fmt.Sprintf("'%s'", s))
	}

	queryStr := fmt.Sprintf(`SELECT
	concat_ws(':',f.instance,t.ID) AS inst
FROM
	information_schema.cluster_processlist t
LEFT JOIN information_schema.cluster_info f ON
	t.INSTANCE = f.STATUS_ADDRESS
WHERE
	t.user in (%s)`, strings.Join(users, ","))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if duration > 0 {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(ctx, time.Duration(duration)*time.Second)
		defer cancelFunc()
	}

	var sessionCounts atomic.Uint64

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM) // 捕获 Ctrl+C 信号

	round := 0
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				utime := time.Now()
				_, results, err := db.GeneralQuery(ctx, queryStr)
				if err != nil {
					logger.Error("query error", zap.String("query", queryStr), zap.Error(err))
					cancel()
					return
				}

				counts := len(results)
				logger.Info(fmt.Sprintf("started round [%d] kill username sql digests session operation...", round))
				logger.Info(fmt.Sprintf("killed username sql digests session generate list finished in %fs, session counts [%d] ", time.Since(utime).Seconds(), counts))

				if counts == 0 {
					logger.Info(fmt.Sprintf("completed round [%d] kill username sql digests session counts [%d] operation finished in %fs", round, sessionCounts.Load(), time.Since(utime).Seconds()))
					time.Sleep(time.Duration(interval) * time.Millisecond)
					round++
					continue
				}

				sessionCounts.Store(uint64(counts))

				g, gCtx := errgroup.WithContext(ctx)
				g.SetLimit(concurrency)
				for _, res := range results {
					r := res
					g.Go(func() error {
						stime := time.Now()
						instS := strings.Split(r["inst"], ":")
						if _, err := db.ExecContext(gCtx, fmt.Sprintf("kill tidb %s", instS[2])); err != nil {
							return err
						}
						// -1 operation
						sessionCounts.Add(^uint64(0))
						logger.Info(fmt.Sprintf("killed username sql digests session on [%s:%s] with id [%s] finished in %fs, session counts [%d]", instS[0], instS[1], instS[2], time.Since(stime).Seconds(), sessionCounts.Load()))
						return nil
					})
				}
				if err := g.Wait(); err != nil {
					logger.Error("kill error", zap.Error(err))
					cancel()
					return
				}

				logger.Info(fmt.Sprintf("completed round [%d] kill username sql digests session counts [%d] operation finished in %fs", round, sessionCounts.Load(), time.Since(utime).Seconds()))
				time.Sleep(time.Duration(interval) * time.Millisecond)
				round++
			}
		}
	}(ctx)

	select {
	case <-sigChan:
		cancel()
		logger.Error("signal error", zap.Error(errors.New("receive Ctrl+C signal, interrupt program execution")))
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			logger.Error("timeout error", zap.Error(fmt.Errorf("the running time has expired [--duration %d] and the program automatically exited", duration)))
		} else {
			logger.Error("cancel error", zap.Error(errors.New("the program was canceled by error cancel, the program ends automatically")))
		}
	}
	return nil
}
