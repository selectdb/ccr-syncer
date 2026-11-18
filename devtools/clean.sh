#!/usr/bin/env bash

rm -rf ccr.db
echo "drop table ccr.src_1;" | mysql -h 127.0.0.1 -P 29030 -uroot
