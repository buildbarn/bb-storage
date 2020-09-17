module github.com/buildbarn/bb-storage

go 1.14

require (
	cloud.google.com/go/storage v1.10.0
	contrib.go.opencensus.io/exporter/jaeger v0.2.0
	contrib.go.opencensus.io/exporter/prometheus v0.2.0
	contrib.go.opencensus.io/exporter/stackdriver v0.12.9
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/DataDog/sketches-go v0.0.0-20190923095040-43f19ad77ff7 // indirect
	github.com/aws/aws-sdk-go v1.33.12
	github.com/bazelbuild/remote-apis v0.0.0-20200708200203-1252343900d9
	github.com/benbjohnson/clock v1.0.3 // indirect
	github.com/go-redis/redis/v8 v8.0.0
	github.com/go-redis/redisext v0.1.7
	github.com/golang/mock v1.4.4-0.20200406172829-6d816de489c1
	github.com/golang/protobuf v1.4.2
	github.com/google/go-jsonnet v0.16.0
	github.com/google/uuid v1.1.1
	github.com/gordonklaus/ineffassign v0.0.0-20200309095847-7953dde2c7bf // indirect
	github.com/gorilla/mux v1.7.4
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/lazybeaver/xorshift v0.0.0-20170702203709-ce511d4823dd
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/prometheus/client_golang v1.7.1
	github.com/stretchr/testify v1.6.1
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	go.opencensus.io v0.22.4
	gocloud.dev v0.20.0
	golang.org/x/net v0.0.0-20200707034311-ab3426394381
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sys v0.0.0-20200727154430-2d971f7391a4
	google.golang.org/genproto v0.0.0-20200726014623-da3ae01ef02d
	google.golang.org/grpc v1.31.0
)
