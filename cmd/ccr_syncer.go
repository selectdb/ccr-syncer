package main

import (
	"flag"
	"log"
	"sync"
	"time"

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
	// Step 1: Check db
	if dbPath == "" {
		log.Fatal("db_dir is empty")
	}
	db, err := storage.NewSQLiteDB(dbPath)
	if err != nil {
		log.Fatalf("new sqlite db error: %+v", err)
	}

	// Step 2: create job manager && http service
	jobManager := ccr.NewJobManager(db)
	httpService := service.NewHttpServer(9190, db, jobManager)

	// Step 3: http service start
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := httpService.Start(); err != nil {
			log.Fatalf("http service start error: %+v", err)
		}
	}()
	time.Sleep(1 * time.Second) // only for check http service start, if not, will log.Fatal

	// Step 4: job manager recover
	if err := jobManager.Recover(); err != nil {
		log.Fatalf("recover job manager error: %+v", err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		jobManager.Start()
	}()

	// Step 5: wait for all task done
	wg.Wait()
}
