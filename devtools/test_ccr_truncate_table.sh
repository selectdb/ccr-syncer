#!/bin/bash

curl -X POST -H "Content-Type: application/json" -d '{
    "name": "ccr_truncate_table",
    "src": {
      "host": "localhost",
      "port": "9030",
      "thrift_port": "9020",
      "user": "root",
      "password": "",
      "database": "ccr",
      "table": "truncate"
    },
    "dest": {
      "host": "localhost",
      "port": "9030",
      "thrift_port": "9020",
      "user": "root",
      "password": "",
      "database": "ccr",
      "table": "truncate_alias"
    }
}' http://127.0.0.1:9190/create_ccr
