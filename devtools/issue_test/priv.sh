#!/usr/bin/env bash

curl -X POST -H "Content-Type: application/json" -d '{
    "name": "priv_test",
    "src": {
      "host": "localhost",
      "port": "9030",
      "thrift_port": "9020",
      "user": "etl",
      "password": "etl%2023",
      "database": "tmp",
      "table": "ccr_test_src"
    },
    "dest": {
      "host": "localhost",
      "port": "9030",
      "thrift_port": "9020",
      "user": "etl",
      "password": "etl%2023",
      "database": "tmp",
      "table": "ccr_test_dst"
    }
}' http://127.0.0.1:9190/create_ccr
