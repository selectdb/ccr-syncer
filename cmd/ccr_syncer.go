package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/selectdb/ccr_syncer/ccr"
	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/rpc"
	"github.com/selectdb/ccr_syncer/service"
	"github.com/selectdb/ccr_syncer/storage"
	"github.com/selectdb/ccr_syncer/utils"
)

type Syncer struct {
	Host string
	Port int
}

var (
	dbPath string
	syncer Syncer
)

func init() {
	flag.StringVar(&dbPath, "db_dir", "ccr.db", "sqlite3 db file")
	flag.StringVar(&syncer.Host, "host", "127.0.0.1", "syncer host")
	flag.IntVar(&syncer.Port, "port", 9190, "syncer port")
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

	// Step 2: init factory
	factory := ccr.NewFactory(rpc.NewRpcFactory(), ccr.NewMetaFactory(), base.NewISpecFactory())

	// Step 3: create job manager && http service && checker
	hostInfo := fmt.Sprintf("%s:%d", syncer.Host, syncer.Port)
	jobManager := ccr.NewJobManager(db, factory, hostInfo)
	httpService := service.NewHttpServer(syncer.Host, syncer.Port, db, jobManager)
	checker := ccr.NewChecker(hostInfo, db, jobManager)

	// Step 4: http service start
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := httpService.Start(); err != nil {
			log.Fatalf("http service start error: %+v", err)
		}
	}()
	time.Sleep(1 * time.Second) // only for check http service start, if not, will log.Fatal

	// Step 5: start job manager
	wg.Add(1)
	go func() {
		defer wg.Done()
		jobManager.Start()
	}()

	// Step 6: start checker
	wg.Add(1)
	go func() {
		defer wg.Done()
		checker.Start()
	}()

	// Step 6: wait for all task done
	wg.Wait()
}
