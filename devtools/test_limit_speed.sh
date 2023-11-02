#!/bin/bash

curl -X POST -H "Content-Type: application/json" -d '{
    "name": "test_speed_limit",
    "src": {
      "host": "localhost",
      "port": "9030",
      "thrift_port": "9020",
      "user": "root",
      "password": "",
      "database": "ccr",
      "table": "github_test_1"
    },
    "dest": {
      "host": "localhost",
      "port": "9030",
      "thrift_port": "9020",
      "user": "root",
      "password": "",
      "database": "dccr",
      "table": "github_test_1_sync"
    }
}' http://127.0.0.1:9190/create_ccr
