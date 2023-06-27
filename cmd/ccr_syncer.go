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
