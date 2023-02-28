#!/usr/bin/env bash

kitex -module github.com/selectdb/ccr_syncer thrift/FrontendService.thrift
kitex -module github.com/selectdb/ccr_syncer thrift/BackendService.thrift
