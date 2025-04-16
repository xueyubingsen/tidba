package main

import (
	"context"

	"github.com/wentaojin/tidba/database/mysql"
	"github.com/wentaojin/tidba/model"
)

func main() {
	ctx := context.Background()

	db, err := mysql.NewDatabase(ctx, "root:@tcp(120.92.108.85:4000)/?pingcap")
	if err != nil {
		panic(err)
	}
	defer db.DB.Close()

	cols, res, err := db.GeneralQuery(ctx, "explain analyze select * from pingcap.sbtest1 limit 1")
	if err != nil {
		panic(err)
	}
	model.QueryResultFormatTableStyle(cols, res)

	// g := &errgroup.Group{}
	// g.SetLimit(8)
	// for i := 0; i < 5000000; i++ {
	// 	g.Go(func() error {
	// 		if _, err := db.ExecContext(ctx, `INSERT INTO pingcap.t11 values (?,?,?)`, fmt.Sprintf("fs%d", i), i, i); err != nil {
	// 			return err
	// 		}
	// 		return nil
	// 	})
	// }
	// if err := g.Wait(); err != nil {
	// 	panic(err)
	// }
}
