module github.com/buildbarn/bb-storage

go 1.18

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

replace mvdan.cc/gofumpt => mvdan.cc/gofumpt v0.3.0

require (
	github.com/aws/aws-sdk-go-v2 v1.15.0
	github.com/aws/aws-sdk-go-v2/config v1.15.0
	github.com/aws/aws-sdk-go-v2/credentials v1.10.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.26.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.0
	github.com/bazelbuild/remote-apis v0.0.0-20220223171137-04784f4a830c
	github.com/go-redis/redis/extra/redisotel v0.3.0
	github.com/go-redis/redis/v8 v8.11.4
	github.com/golang/mock v1.6.0
	github.com/google/go-jsonnet v0.18.0
	github.com/google/uuid v1.3.0
	github.com/gordonklaus/ineffassign v0.0.0-20210914165742-4cc7213b9bc8 // indirect; GitHub Workflow
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/jmespath/go-jmespath v0.4.0
	github.com/klauspost/compress v1.15.1
	github.com/lazybeaver/xorshift v0.0.0-20170702203709-ce511d4823dd
	github.com/prometheus/client_golang v1.12.1
	github.com/stretchr/testify v1.7.1
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.29.0
	go.opentelemetry.io/contrib/propagators/b3 v1.4.0
	go.opentelemetry.io/otel v1.4.1
	go.opentelemetry.io/otel/exporters/jaeger v1.4.1
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.4.1
	go.opentelemetry.io/otel/sdk v1.4.1
	go.opentelemetry.io/otel/trace v1.4.1
	go.opentelemetry.io/proto/otlp v0.12.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220315194320-039c03cc5b86
	google.golang.org/genproto v0.0.0-20220314164441-57ef72a4c106
	google.golang.org/grpc v1.45.0
	google.golang.org/protobuf v1.27.1
	mvdan.cc/gofumpt v0.3.0 // indirect; GitHub Workflow
)

require (
	cloud.google.com/go v0.100.1 // indirect
	dmitri.shuralyov.com/go/generated v0.0.0-20211227232225-c5b6cf572ec5 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.0 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.13.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.0 // indirect
	github.com/aws/smithy-go v1.11.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.2.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-redis/redis/extra/rediscmd v0.2.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/stretchr/objx v0.3.0 // indirect
	golang.org/x/mod v0.5.1 // indirect
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f // indirect
	golang.org/x/oauth2 v0.0.0-20220309155454-6242fa91716a // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.9 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)
