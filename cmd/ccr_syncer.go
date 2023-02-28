package main

import (
	"sync"

	"github.com/selectdb/ccr_syncer/ccr"
	"github.com/selectdb/ccr_syncer/service"
	"github.com/selectdb/ccr_syncer/storage"
	"github.com/selectdb/ccr_syncer/utils"
)

func init() {
	utils.InitLog()
}

// func main() {
// 	// create src dest table spec, and create creator
// 	src := ccr.Spec{
// 		Host:     "localhost",
// 		Port:     "9030",
// 		User:     "root",
// 		Password: "",
// 		Database: "demo",
// 		Table:    "example_tbl",
// 	}
// 	dest := ccr.Spec{
// 		Host:     "localhost",
// 		Port:     "9030",
// 		User:     "root",
// 		Password: "",
// 		Database: "ccrt",
// 		Table:    "copy",
// 	}

// 	rpc.NewThriftRpc(&src)

// 	if creator, err := ccr.NewCreator(src, dest); err != nil {
// 		log.Error(err)
// 		panic(err)
// 	} else {
// 		// create ccr
// 		if err := creator.Create(); err != nil {
// 			panic(err)
// 		}
// 	}
// 	//  else {
// 	// 	// show create table
// 	// 	if table, stmt, err := creator.SrcShowCreateTable(); err != nil {
// 	// 		panic(err)
// 	// 	} else {
// 	// 		println(table)
// 	// 		println(stmt)

// 	// 		// replace stmt by dest
// 	// 		new_stmt := creator.ReplaceSrcCreateTableStmt(stmt)
// 	// 		println(new_stmt)
// 	// 	}

// 	// }
// }

func main() {
	db, err := storage.NewSQLiteDB("ccr.db")
	if err != nil {
		panic(err)
	}

	job_manager := ccr.NewJobManager(db)
	httpService := service.NewHttpServer(9190, db, job_manager)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		job_manager.Start()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		httpService.Start()
	}()
	wg.Wait()
}
