# set makefile echo back
ifdef VERBOSE
	V :=
else
	V := @
endif

tag := $(shell git describe --abbrev=0 --always --dirty --tags)
sha := $(shell git rev-parse --short HEAD)
git_tag_sha := $(tag):$(sha)
LDFLAGS="-X 'github.com/selectdb/ccr_syncer/pkg/version.GitTagSha=$(git_tag_sha)'"
GOFLAGS=

# Check formatter, if exist gofumpt, use it
GOFUMPT := $(shell command -v gofumpt 2> /dev/null)
ifdef GOFUMPT
	GOFORMAT := gofumpt -s -w
else
	GOFORMAT := go fmt
endif

# COVERAGE=ON make
ifeq ($(COVERAGE),ON)
    GOFLAGS += -cover
endif

.PHONY: flag_coverage
## COVERAGE=ON : Set coverage flag

.PHONY: default
## default: Build ccr_syncer
default: ccr_syncer

.PHONY: build
## build : Build binary
build: ccr_syncer get_binlog ingest_binlog get_meta snapshot_op get_master_token spec_checker rows_parse

.PHONY: bin
## bin : Create bin directory
bin:
	@mkdir -p bin

.PHONY: lint
## lint : Lint codespace
# TODO(Drogon): add golang lint
lint:
	$(V)golangci-lint run

.PHONY: fmt
## fmt : Format all code
fmt:
	$(V)$(GOFORMAT) .

.PHONY: test
## test : Run test
test:
	$(V)go test $(shell go list ./... | grep -v github.com/selectdb/ccr_syncer/cmd | grep -v github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/) | grep -F -v '[no test files]'

.PHONY: help
## help : Print help message
help: Makefile
	@sed -n 's/^##//p' $< | awk 'BEGIN {FS = ":"} {printf "\033[36m%-23s\033[0m %s\n", $$1, $$2}'


# --------------- ------------------ ---------------
# --------------- User Defined Tasks ---------------

.PHONY: cloc
## cloc : Count lines of code
cloc:
	$(V)tokei -C . -e pkg/rpc/kitex_gen -e pkg/rpc/thrift

.PHONY: gen_mock
## gen_mock : Generate mock
gen_mock:
	$(V)mockgen -source=pkg/rpc/fe.go -destination=pkg/ccr/fe_mock.go -package=ccr
	$(V)mockgen -source=pkg/ccr/metaer.go -destination=pkg/ccr/metaer_mock.go -package=ccr
	$(V)mockgen -source=pkg/ccr/metaer_factory.go -destination=pkg/ccr/metaer_factory_mock.go -package=ccr
	$(V)mockgen -source=pkg/rpc/rpc_factory.go -destination=pkg/ccr/rpc_factory_mock.go -package=ccr

.PHONY: ccr_syncer
## ccr_syncer : Build ccr_syncer binary
ccr_syncer: bin
	$(V)go build ${GOFLAGS} -ldflags ${LDFLAGS} -o bin/ccr_syncer ./cmd/ccr_syncer

.PHONY: get_binlog
## get_binlog : Build get_binlog binary
get_binlog: bin
	$(V)go build -o bin/get_binlog ./cmd/get_binlog

## run_get_binlog : Run get_binlog binary
run_get_binlog: get_binlog
	$(V)bin/get_binlog

.PHONY: sync_thrift
## sync_thrift : Sync thrift
sync_thrift:
	$(V)rsync -avc $(THRIFT_DIR)/ pkg/rpc/thrift/
	$(V)$(MAKE) -C pkg/rpc/ gen_thrift

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

.PHONY: thrift_get_meta
## thrift_get_meta : Build thrift_get_meta binary
thrift_get_meta: bin
	$(V)go build -o bin/thrift_get_meta ./cmd/thrift_get_meta

.PHONY: metrics
## metrics : Build metrics binary
metrics: bin
	$(V)go build -o bin/metrics ./cmd/metrics

.PHONY: todos
## todos : Print all todos
todos:
	$(V)grep -rnw . -e "TODO" | grep -v '^./pkg/rpc/thrift' | grep -v '^./.git'
