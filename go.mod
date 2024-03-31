module github.com/buildbarn/bb-storage

go 1.21

// Using the most recent version causes a cyclic dependency in protoc.
replace google.golang.org/protobuf => google.golang.org/protobuf v1.32.0

require (
	cloud.google.com/go/longrunning v0.5.6
	cloud.google.com/go/storage v1.40.0
	github.com/aohorodnyk/mimeheader v0.0.6
	github.com/aws/aws-sdk-go-v2 v1.26.1
	github.com/aws/aws-sdk-go-v2/config v1.27.10
	github.com/aws/aws-sdk-go-v2/credentials v1.17.10
	github.com/aws/aws-sdk-go-v2/service/s3 v1.53.1
	github.com/aws/aws-sdk-go-v2/service/sts v1.28.6
	github.com/bazelbuild/buildtools v0.0.0-20240313121412-66c605173954
	github.com/bazelbuild/remote-apis v0.0.0-20240319211552-96942a2107c7
	github.com/fxtlabs/primes v0.0.0-20150821004651-dad82d10a449
	github.com/go-jose/go-jose/v3 v3.0.3
	github.com/golang/mock v1.6.0
	github.com/google/go-jsonnet v0.20.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/jmespath/go-jmespath v0.4.0
	github.com/klauspost/compress v1.17.7
	github.com/lazybeaver/xorshift v0.0.0-20170702203709-ce511d4823dd
	github.com/prometheus/client_golang v1.19.0
	github.com/prometheus/client_model v0.6.0
	github.com/prometheus/common v0.51.1
	github.com/sercand/kuberesolver/v5 v5.1.1
	github.com/stretchr/testify v1.9.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0
	go.opentelemetry.io/contrib/propagators/b3 v1.24.0
	go.opentelemetry.io/otel v1.24.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.24.0
	go.opentelemetry.io/otel/sdk v1.24.0
	go.opentelemetry.io/otel/trace v1.24.0
	go.opentelemetry.io/proto/otlp v1.1.0
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616
	golang.org/x/oauth2 v0.18.0
	golang.org/x/sync v0.6.0
	golang.org/x/sys v0.18.0
	google.golang.org/api v0.172.0
	google.golang.org/genproto/googleapis/bytestream v0.0.0-20240325203815-454cdb8f5daa
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240325203815-454cdb8f5daa
	google.golang.org/grpc v1.62.1
	google.golang.org/protobuf v1.33.0
	mvdan.cc/gofumpt v0.6.0
)

require (
	cloud.google.com/go v0.112.2 // indirect
	cloud.google.com/go/compute v1.25.1 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.7 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.2 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.11.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.3.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.11.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.17.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.20.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.23.4 // indirect
	github.com/aws/smithy-go v1.20.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.3 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.19.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/procfs v0.13.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/mod v0.16.0 // indirect
	golang.org/x/net v0.22.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.19.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20240325203815-454cdb8f5daa // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240325203815-454cdb8f5daa // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)
