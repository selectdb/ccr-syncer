#!/bin/bash

curl -X POST -H "Content-Type: application/json" -d '{
    "name": "ccr_test",
    "src": {
      "host": "localhost",
      "port": "9030",
      "thrift_port": "9020",
      "user": "root",
      "password": "",
      "database": "ccr",
      "table": "src_1"
    },
    "dest": {
      "host": "localhost",
      "port": "29030",
      "thrift_port": "29020",
      "user": "root",
      "password": "",
      "database": "ccr",
      "table": "src_1"
    }
}' http://127.0.0.1:9190/create_ccr
