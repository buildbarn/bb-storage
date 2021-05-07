module github.com/buildbarn/bb-storage

go 1.16

replace github.com/golang/mock => github.com/golang/mock v1.4.4-0.20201026142858-99aa9272d551

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	contrib.go.opencensus.io/exporter/jaeger v0.2.1
	contrib.go.opencensus.io/exporter/prometheus v0.3.0
	contrib.go.opencensus.io/exporter/stackdriver v0.13.5
	dmitri.shuralyov.com/go/generated v0.0.0-20170818220700-b1254a446363 // indirect
	github.com/aws/aws-sdk-go v1.37.28
	github.com/bazelbuild/remote-apis v0.0.0-20210505181611-ce7036ef5417
	github.com/go-redis/redis/extra/redisotel v0.3.0
	github.com/go-redis/redis/v8 v8.7.1
	github.com/golang/mock v1.4.3
	github.com/google/go-jsonnet v0.17.0
	github.com/google/uuid v1.2.0
	github.com/gordonklaus/ineffassign v0.0.0-20210225214923-2e10b2664254 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/lazybeaver/xorshift v0.0.0-20170702203709-ce511d4823dd
	github.com/prometheus/client_golang v1.9.0
	github.com/stretchr/testify v1.7.0
	go.opencensus.io v0.23.0
	golang.org/x/net v0.0.0-20210226172049-e18ecbb05110
	golang.org/x/sys v0.0.0-20210309074719-68d13333faf2
	google.golang.org/genproto v0.0.0-20210310155132-4ce2db91004e
	google.golang.org/grpc v1.36.0
	google.golang.org/protobuf v1.25.0
	mvdan.cc/gofumpt v0.1.1 // indirect
)
