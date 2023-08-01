#!/bin/bash

curl -X POST -H "Content-Type: application/json" -d '{
    "name": "ccr_test"
}' http://127.0.0.1:9190/resume
