module github.com/selectdb/ccr_syncer

go 1.20

require (
	github.com/apache/thrift v0.19.0
	github.com/cloudwego/kitex v0.8.0
	github.com/go-sql-driver/mysql v1.7.1
	github.com/hashicorp/go-metrics v0.5.3
	github.com/keepeye/logrus-filename v0.0.0-20190711075016-ce01a4391dd1
	github.com/mattn/go-sqlite3 v1.14.22
	github.com/modern-go/gls v0.0.0-20220109145502-612d0167dce5
	github.com/prometheus/client_golang v1.18.0
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.8.4
	github.com/t-tomalak/logrus-prefixed-formatter v0.5.2
	github.com/tidwall/btree v1.7.0
	go.uber.org/mock v0.4.0
	golang.org/x/exp v0.0.0-20240213143201-ec583247a57a
	gopkg.in/natefinch/lumberjack.v2 v2.2.1

)

// dependabot
require golang.org/x/net v0.21.0 // indirect; https://github.com/selectdb/ccr-syncer/security/dependabot/2

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bufbuild/protocompile v0.8.0 // indirect
	github.com/bytedance/gopkg v0.0.0-20240202110943-5e26950c5e57 // indirect
	github.com/bytedance/sonic v1.11.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/chenzhuoyu/base64x v0.0.0-20230717121745-296ad89f973d // indirect
	github.com/chenzhuoyu/iasm v0.9.1 // indirect
	github.com/choleraehyq/pid v0.0.18 // indirect
	github.com/cloudwego/configmanager v0.2.0 // indirect
	github.com/cloudwego/dynamicgo v0.2.0 // indirect
	github.com/cloudwego/fastpb v0.0.4 // indirect
	github.com/cloudwego/frugal v0.1.13 // indirect
	github.com/cloudwego/localsession v0.0.2 // indirect
	github.com/cloudwego/netpoll v0.5.1 // indirect
	github.com/cloudwego/thriftgo v0.3.6 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/fatih/structtag v1.2.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/pprof v0.0.0-20240207164012-fb44976bdcd5 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/jhump/protoreflect v1.15.6 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.2.6 // indirect
        github.com/lib/pq v1.10.9 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/oleiade/lane v1.0.1 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.27.8 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.0 // indirect
	github.com/prometheus/common v0.47.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/tidwall/gjson v1.17.1 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/x-cray/logrus-prefixed-formatter v0.5.2 // indirect
	golang.org/x/arch v0.7.0 // indirect
	golang.org/x/crypto v0.19.0 // indirect
	golang.org/x/sync v0.6.0 // indirect
	golang.org/x/sys v0.17.0 // indirect
	golang.org/x/term v0.17.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240213162025-012b6fc9bca9 // indirect
	google.golang.org/grpc v1.60.1 // indirect
	google.golang.org/protobuf v1.32.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/apache/thrift => github.com/apache/thrift v0.13.0
