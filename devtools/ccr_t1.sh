#!/bin/bash

# use curl to post json rpc to 127.0.0.1:9190/create_ccr
# json map to below golang struct
# type Spec struct {
# 	Host       string `json:"host"`
# 	Port       string `json:"port"`
# 	ThriftPort string `json:"thrift_port"`
# 	User       string `json:"user"`
# 	Password   string `json:"password"`
# 	Cluster    string `json:"cluster"`
# 	Database   string `json:"database"`
# 	Table      string `json:"table"`
# }
# type CreateCcrRequest struct {
# 	Src  ccr.Spec `json:"src"`
# 	Dest ccr.Spec `json:"dest"`
# }
# src := ccr.Spec{
#     Host:     "localhost",
#     Port:     "9030",
#     User:     "root",
#     Password: "",
#     Database: "demo",
#     Table:    "example_tbl",
# }
# dest := ccr.Spec{
#     Host:     "localhost",
#     Port:     "9030",
#     User:     "root",
#     Password: "",
#     Database: "ccrt",
#     Table:    "copy",
# }

curl -X POST -H "Content-Type: application/json" -d '{
    "name": "ccr_test",
    "src": {
      "host": "localhost",
      "port": "9030",
      "thrift_port": "9020",
      "user": "root",
      "password": "",
      "database": "c1",
      "table": "t1"
    },
    "dest": {
      "host": "localhost",
      "port": "29030",
      "thrift_port": "29020",
      "user": "root",
      "password": "",
      "database": "c1",
      "table": "t1"
    }
}' http://127.0.0.1:9190/create_ccr
