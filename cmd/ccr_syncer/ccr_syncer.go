// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	"github.com/selectdb/ccr_syncer/pkg/service"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/version"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

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
	Pprof       bool
	Ppof_port   int
}

var (
	dbPath       string
	syncer       Syncer
	printVersion bool
)

func init() {
	flag.BoolVar(&printVersion, "version", false, "The program's version")

	flag.StringVar(&dbPath, "db_dir", "ccr.db", "sqlite3 db file")
	flag.StringVar(&syncer.Db_type, "db_type", "sqlite3", "meta db type")
	flag.StringVar(&syncer.Db_host, "db_host", "127.0.0.1", "meta db host")
	flag.IntVar(&syncer.Db_port, "db_port", 3306, "meta db port")
	flag.StringVar(&syncer.Db_user, "db_user", "root", "meta db user")
	flag.StringVar(&syncer.Db_password, "db_password", "", "meta db password")

	flag.StringVar(&syncer.Host, "host", "127.0.0.1", "syncer host")
	flag.IntVar(&syncer.Port, "port", 9190, "syncer port")
	flag.IntVar(&syncer.Ppof_port, "pprof_port", 6060, "pprof port used for memory analyze")
	flag.BoolVar(&syncer.Pprof, "pprof", false, "use pprof or not")
	flag.Parse()

	utils.InitLog()
}

func main() {
	if printVersion {
		fmt.Println(version.GetVersion())
		os.Exit(0)
	}

	// print version
	log.Infof("ccr start, version: %s", version.GetVersion())

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
	case "postgresql":
		db, err = storage.NewPostgresqlDB(syncer.Db_host, syncer.Db_port, syncer.Db_user, syncer.Db_password)
	default:
		err = xerror.Wrap(err, xerror.Normal, "new meta db failed.")
	}
	if err != nil {
		log.Fatalf("new meta db error: %+v", err)
	}

	// Step 2: init factory
	factory := ccr.NewFactory(rpc.NewRpcFactory(), ccr.NewMetaFactory(), base.NewSpecerFactory(), ccr.DefaultThriftMetaFactory)

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

	// Step 8: start monitor
	monitor := NewMonitor(jobManager)
	wg.Add(1)
	go func() {
		defer wg.Done()
		monitor.Start()
	}()

	// Step 9: start signal mux
	// use closure to capture httpService, checker, jobManager
	signalHandler := func(signal os.Signal) bool {
		switch signal {
		case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			log.Infof("handle signal: %s", signal.String())
			// stop httpService first, denied new request
			httpService.Stop()
			checker.Stop()
			jobManager.Stop()
			monitor.Stop()
			log.Info("all service stop")
			return true
		case syscall.SIGHUP:
			log.Infof("receive signal: %s", signal.String())
			return false
		default:
			log.Infof("receive signal: %s", signal.String())
			return false
		}
	}
	signalMux := NewSignalMux(signalHandler)
	wg.Add(1)
	go func() {
		defer wg.Done()
		signalMux.Serve()
	}()

	// Step 10: start pprof
	if syncer.Pprof == true {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var pprof_info string = fmt.Sprintf("%s:%d", syncer.Host, syncer.Ppof_port)
			if err := http.ListenAndServe(pprof_info, nil); err != nil {
				log.Infof("start pprof failed on: %s, error : %+v", pprof_info, err)
			}
		}()
	}

	// Step 11: wait for all task done
	wg.Wait()
}
