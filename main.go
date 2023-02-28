package main

import (
	"os"

	"github.com/selectdb/ccr_syncer/ccr"
	log "github.com/sirupsen/logrus"
)

func init_log() {
	// TODO(Drogon): config logrus
	// init logrus
	// Log as JSON instead of the default ASCII formatter.
	//   log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// log the debug severity or above.
	log.SetLevel(log.DebugLevel)
}

func init() {
	init_log()
}

func main() {
	// create src dest table spec, and create creator
	src := ccr.TableSpec{
		Host:     "localhost",
		Port:     "9030",
		User:     "root",
		Password: "",
		Database: "demo",
		Table:    "example_tbl",
	}
	dest := ccr.TableSpec{
		Host:     "localhost",
		Port:     "9030",
		User:     "root",
		Password: "",
		Database: "ccrt",
		Table:    "copy",
	}

	if creator, err := ccr.NewCreator(src, dest); err != nil {
		log.Error(err)
		panic(err)
	} else {
		// create ccr
		if err := creator.Create(); err != nil {
			panic(err)
		}
	}
	//  else {
	// 	// show create table
	// 	if table, stmt, err := creator.SrcShowCreateTable(); err != nil {
	// 		panic(err)
	// 	} else {
	// 		println(table)
	// 		println(stmt)

	// 		// replace stmt by dest
	// 		new_stmt := creator.ReplaceSrcCreateTableStmt(stmt)
	// 		println(new_stmt)
	// 	}

	// }
}
