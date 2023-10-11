package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/selectdb/ccr_syncer/ccr"
	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/rpc"
	"github.com/selectdb/ccr_syncer/service"
	"github.com/selectdb/ccr_syncer/storage"
	"github.com/selectdb/ccr_syncer/utils"
	"github.com/selectdb/ccr_syncer/xerror"

	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/go-metrics/prometheus"
	log "github.com/sirupsen/logrus"
)

type Syncer struct {
	Host string
	Port int

	Db_type     string
	Db_host     string
	Db_port     int
	Db_user     string
	Db_password string
}

var (
	dbPath  string
	syncer  Syncer
	version bool
)

func init() {
	flag.BoolVar(&version, "version", false, "The program's version")

	flag.StringVar(&dbPath, "db_dir", "ccr.db", "sqlite3 db file")
	flag.StringVar(&syncer.Db_type, "db_type", "sqlite3", "meta db type")
	flag.StringVar(&syncer.Db_host, "db_host", "127.0.0.1", "meta db host")
	flag.IntVar(&syncer.Db_port, "db_port", 3306, "meta db port")
	flag.StringVar(&syncer.Db_user, "db_user", "root", "meta db user")
	flag.StringVar(&syncer.Db_password, "db_password", "", "meta db password")

	flag.StringVar(&syncer.Host, "host", "127.0.0.1", "syncer host")
	flag.IntVar(&syncer.Port, "port", 9190, "syncer port")
	flag.Parse()

	utils.InitLog()
}

func main() {
	if version {
		printVersion()
	}

	// print version
	log.Infof("ccr start, version: %s", getVersion())

	// Step 1: Check db
	if dbPath == "" {
		log.Fatal("db_dir is empty")
	}
	var db storage.DB
	var err error
	switch syncer.Db_type {
	case "sqlite3":
		db, err = storage.NewSQLiteDB(dbPath)
	case "mysql":
		db, err = storage.NewMysqlDB(syncer.Db_host, syncer.Db_port, syncer.Db_user, syncer.Db_password)
	default:
		err = xerror.Wrap(err, xerror.Normal, "new meta db failed.")
	}
	if err != nil {
		log.Fatalf("new meta db error: %+v", err)
	}

	// Step 2: init factory
	factory := ccr.NewFactory(rpc.NewRpcFactory(), ccr.NewMetaFactory(), base.NewSpecerFactory())

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

	// Step 7: init metrics
	sink, err := prometheus.NewPrometheusSink()
	if err != nil {
		log.Fatalf("new prometheus sink failed: %+v", err)
	}
	metrics.NewGlobal(metrics.DefaultConfig("ccr-metrics"), sink)

	// Step 8: wait for all task done
	wg.Wait()
}
