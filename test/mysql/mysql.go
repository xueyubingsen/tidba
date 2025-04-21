package main

import (
	"context"

	"github.com/wentaojin/tidba/database/mysql"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.Background()

	db, err := mysql.NewDatabase(ctx, "root:@tcp(120.92.108.85:4000)/?pingcap")
	if err != nil {
		panic(err)
	}
	defer db.DB.Close()

	// cols, res, err := db.GeneralQuery(ctx, "explain analyze select * from pingcap.sbtest1 limit 1")
	// if err != nil {
	// 	panic(err)
	// }
	// model.QueryResultFormatTableStyle(cols, res)

	g := &errgroup.Group{}
	g.SetLimit(32)
	for i := 0; i < 5000000; i++ {
		g.Go(func() error {
			// if _, err := db.ExecContext(ctx, `INSERT INTO pingcap.t11 values (?,?,?)`, fmt.Sprintf("fs%d", i), i, i); err != nil {
			// 	return err
			// }
			rows, err := db.QueryContext(ctx, `select id from sbtest.sbtest1 where id >= 5000 and id <= 10000`)
			if err != nil {
				return err
			}
			defer rows.Close()

			var ids []int
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					return err
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil {
				return err
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		panic(err)
	}
}
