package main

import (
	"flag"
	"log"
	"sync"

	"github.com/selectdb/ccr_syncer/ccr"
	"github.com/selectdb/ccr_syncer/service"
	"github.com/selectdb/ccr_syncer/storage"
	"github.com/selectdb/ccr_syncer/utils"
)

var (
	dbPath string
)

func init() {
	flag.StringVar(&dbPath, "db_dir", "ccr.db", "sqlite3 db file")
	flag.Parse()

	utils.InitLog()
}

func main() {
	if dbPath == "" {
		log.Fatal("db_dir is empty")
	}
	db, err := storage.NewSQLiteDB(dbPath)
	if err != nil {
		log.Fatalf("new sqlite db error: %+v", err)
	}

	job_manager := ccr.NewJobManager(db)
	if err := job_manager.Recover(); err != nil {
		log.Fatalf("recover job manager error: %+v", err)
	}

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
