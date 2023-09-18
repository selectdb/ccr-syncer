# set makefile echo back
ifdef VERBOSE
	V :=
else
	V := @
endif

.PHONY: build
## build : Build binary
build: ccr_syncer get_binlog ingest_binlog get_meta snapshot_op get_master_token spec_checker rows_parse

.PHONY: bin
## bin : Create bin directory
bin:
	@mkdir -p bin

.PHONY: lint
## lint : Lint codespace
lint:
	$(V)cargo clippy --workspace --tests --all-features -- -D warnings

.PHONY: fmt
## fmt : Format all code
fmt:
	$(V)go fmt ./...

.PHONY: test
## test : Run test
test:
	$(V)go test $(shell go list ./... | grep -v github.com/selectdb/ccr_syncer/cmd | grep -v github.com/selectdb/ccr_syncer/rpc/kitex_gen/)

.PHONY: help
## help : Print help message
help: Makefile
	@sed -n 's/^##//p' $< | awk 'BEGIN {FS = ":"} {printf "\033[36m%-23s\033[0m %s\n", $$1, $$2}'


# --------------- ------------------ ---------------
# --------------- User Defined Tasks ---------------
.PHONY: cmd/ccr_syncer
.PHONY: ccr_syncer
## ccr_syncer : Build ccr_syncer binary
ccr_syncer: bin
	$(V)go build -o bin/ccr_syncer ./cmd/ccr_syncer

.PHONY: get_binlog
## get_binlog : Build get_binlog binary
get_binlog: bin
	$(V)go build -o bin/get_binlog ./cmd/get_binlog

## run_get_binlog : Run get_binlog binary
run_get_binlog: get_binlog
	$(V)bin/get_binlog

.PHONY: sync_thrift
## sync_thrift : Sync thrift
# TODO(Drogon): Add build thrift
sync_thrift:
	$(V)rsync -avc $(THRIFT_DIR)/ rpc/thrift/

.PHONY: ingest_binlog
## ingest_binlog : Build ingest_binlog binary
ingest_binlog: bin
	$(V)go build -o bin/ingest_binlog ./cmd/ingest_binlog

.PHONY: get_meta
## get_meta : Build get_meta binary
get_meta: bin
	$(V)go build -o bin/get_meta ./cmd/get_meta

.PHONY: snapshot_op
## snapshot_op : Build snapshot_op binary
snapshot_op: bin
	$(V)go build -o bin/snapshot_op ./cmd/snapshot_op

.PHONY: get_master_token
## get_master_token : Build get_master_token binary
get_master_token: bin
	$(V)go build -o bin/get_master_token ./cmd/get_master_token

.PHONY: spec_checker
## spec_checker : Build spec_checker binary
spec_checker: bin
	$(V)go build -o bin/spec_checker ./cmd/spec_checker

.PHONY: get_lag
## get_lag : Build get_lag binary
get_lag: bin
	$(V)go build -o bin/get_lag ./cmd/get_lag

.PHONY: rows_parse
## rows_parse : Build rows_parse binary
rows_parse: bin
	$(V)go build -o bin/rows_parse ./cmd/rows_parse