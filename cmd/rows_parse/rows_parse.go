package main

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func init() {
	utils.InitLog()
}

func main() {
	src := &base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "56131",
			ThriftPort: "54130",
		},
		User:     "root",
		Password: "",
		Database: "ccr",
		Table:    "",
	}

	db, err := src.Connect()
	if err != nil {
		log.Fatal("connect to doris failed")
	}

	query := "ADMIN SHOW FRONTEND CONFIG LIKE \"%%enable_feature_binlog%%\""
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("query %s failed", query)
	}
	defer rows.Close()

	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			log.Fatal("rows parse failed")
		}
		enable, err := rowParser.GetBool("Value")
		if err != nil {
			log.Fatal("get int64 failed")
		}
		log.Infof("row: %v", enable)
	}
}
