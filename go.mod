module github.com/buildbarn/bb-storage

go 1.24.0

// rules_go doesn't support gomock's package mode.
replace go.uber.org/mock => go.uber.org/mock v0.4.0

require (
	cloud.google.com/go/longrunning v0.6.4
	cloud.google.com/go/storage v1.50.0
	github.com/aohorodnyk/mimeheader v0.0.6
	github.com/aws/aws-sdk-go-v2 v1.36.1
	github.com/aws/aws-sdk-go-v2/config v1.29.6
	github.com/aws/aws-sdk-go-v2/credentials v1.17.59
	github.com/aws/aws-sdk-go-v2/service/s3 v1.76.1
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.14
	github.com/bazelbuild/buildtools v0.0.0-20250204160707-ad48c76ab9b5
	github.com/bazelbuild/remote-apis v0.0.0-20250211041012-7f922028fcfa
	github.com/bazelbuild/rules_go v0.53.0
	github.com/buildbarn/go-sha256tree v0.0.0-20250310211320-0f70f20e855b
	github.com/fxtlabs/primes v0.0.0-20150821004651-dad82d10a449
	github.com/go-jose/go-jose/v3 v3.0.3
	github.com/google/go-jsonnet v0.20.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/jmespath/go-jmespath v0.4.0
	github.com/klauspost/compress v1.17.11
	github.com/lazybeaver/xorshift v0.0.0-20170702203709-ce511d4823dd
	github.com/prometheus/client_golang v1.20.5
	github.com/prometheus/client_model v0.6.1
	github.com/prometheus/common v0.62.0
	github.com/sercand/kuberesolver/v5 v5.1.1
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.59.0
	go.opentelemetry.io/contrib/propagators/b3 v1.34.0
	go.opentelemetry.io/otel v1.34.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.34.0
	go.opentelemetry.io/otel/sdk v1.34.0
	go.opentelemetry.io/otel/trace v1.34.0
	go.opentelemetry.io/proto/otlp v1.5.0
	go.uber.org/mock v0.0.0-00010101000000-000000000000
	golang.org/x/lint v0.0.0-20241112194109-818c5a804067
	golang.org/x/oauth2 v0.26.0
	golang.org/x/sync v0.11.0
	golang.org/x/sys v0.31.0
	google.golang.org/api v0.221.0
	google.golang.org/genproto/googleapis/bytestream v0.0.0-20250212204824-5a70512c5d8b
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250212204824-5a70512c5d8b
	google.golang.org/grpc v1.70.0
	google.golang.org/grpc/security/advancedtls v1.0.0
	google.golang.org/protobuf v1.36.5
	mvdan.cc/gofumpt v0.7.0
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/mock v1.7.0-rc.1 // indirect
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.5.1 // indirect
)

require (
	cel.dev/expr v0.19.0 // indirect
	cloud.google.com/go v0.118.0 // indirect
	cloud.google.com/go/auth v0.14.1 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.7 // indirect
	cloud.google.com/go/compute/metadata v0.6.0 // indirect
	cloud.google.com/go/iam v1.3.1 // indirect
	cloud.google.com/go/monitoring v1.22.1 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.25.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.48.1 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.48.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.8 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.28 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.32 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.32 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.32 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.6.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.14 // indirect
	github.com/aws/smithy-go v1.22.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20240905190251-b4127c9b8d78 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.32.4 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gocql/gocql v1.7.0
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.4 // indirect
	github.com/googleapis/gax-go/v2 v2.14.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.25.1 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.32.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.58.0 // indirect
	go.opentelemetry.io/otel/metric v1.34.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.32.0 // indirect
	golang.org/x/crypto v0.33.0 // indirect
	golang.org/x/mod v0.23.0 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	golang.org/x/time v0.10.0 // indirect
	golang.org/x/tools v0.30.0 // indirect
	google.golang.org/genproto v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250106144421-5f5ef82da422 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)
