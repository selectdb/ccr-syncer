# set makefile echo back
ifdef VERBOSE
	V :=
else
	V := @
endif

.PHONY: build
## build : Build binary
build: ccr_syncer get_binlog ingest_binlog get_meta snapshot_op get_master_token spec_checker

.PHONY: lib
## lib : Create lib directory
lib:
	@mkdir -p lib

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
	$(V)cargo test --workspace

.PHONY: help
## help : Print help message
help: Makefile
	@sed -n 's/^##//p' $< | awk 'BEGIN {FS = ":"} {printf "\033[36m%-23s\033[0m %s\n", $$1, $$2}'


# --------------- ------------------ ---------------
# --------------- User Defined Tasks ---------------
.PHONY: cmd/ccr_syncer
.PHONY: ccr_syncer
## ccr_syncer : Build ccr_syncer binary
ccr_syncer: lib
	$(V)go build -o lib/ccr_syncer cmd/ccr_syncer.go

.PHONY: get_binlog
## get_binlog : Build get_binlog binary
get_binlog: lib
	$(V)go build -o lib/get_binlog cmd/get_binlog.go

## run_get_binlog : Run get_binlog binary
run_get_binlog: get_binlog
	$(V)lib/get_binlog

.PHONY: sync_thrift
## sync_thrift : Sync thrift
# TODO(Drogon): Add build thrift
sync_thrift:
	$(V)rsync -avc $(THRIFT_DIR)/ rpc/thrift/

.PHONY: ingest_binlog
## ingest_binlog : Build ingest_binlog binary
ingest_binlog: lib
	$(V)go build -o lib/ingest_binlog cmd/ingest_binlog.go

.PHONY: get_meta
## get_meta : Build get_meta binary
get_meta: lib
	$(V)go build -o lib/get_meta cmd/get_meta.go

.PHONY: snapshot_op
## snapshot_op : Build snapshot_op binary
snapshot_op: lib
	$(V)go build -o lib/snapshot_op cmd/snapshot_op.go

.PHONY: get_master_token
## get_master_token : Build get_master_token binary
get_master_token: lib
	$(V)go build -o lib/get_master_token cmd/get_master_token.go

.PHONY: spec_checker
## spec_checker : Build spec_checker binary
spec_checker: lib
	$(V)go build -o lib/spec_checker cmd/spec_checker.go