module github.com/buildbarn/bb-storage

go 1.24.5

// rules_go doesn't support gomock's package mode.
replace go.uber.org/mock => go.uber.org/mock v0.4.0

require (
	cloud.google.com/go/longrunning v0.6.7
	cloud.google.com/go/storage v1.55.0
	github.com/aohorodnyk/mimeheader v0.0.6
	github.com/aws/aws-sdk-go-v2 v1.36.5
	github.com/aws/aws-sdk-go-v2/config v1.29.17
	github.com/aws/aws-sdk-go-v2/credentials v1.17.70
	github.com/aws/aws-sdk-go-v2/service/s3 v1.83.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.34.0
	github.com/bazelbuild/buildtools v0.0.0-20250703141324-fdc626802fbc
	github.com/bazelbuild/remote-apis v0.0.0-20250709094936-9ef19c6b5fbf
	github.com/bazelbuild/rules_go v0.55.1
	github.com/buildbarn/go-sha256tree v0.0.0-20250310211320-0f70f20e855b
	github.com/fxtlabs/primes v0.0.0-20150821004651-dad82d10a449
	github.com/go-jose/go-jose/v3 v3.0.4
	github.com/google/go-jsonnet v0.21.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/jmespath/go-jmespath v0.4.0
	github.com/klauspost/compress v1.18.0
	github.com/lazybeaver/xorshift v0.0.0-20170702203709-ce511d4823dd
	github.com/prometheus/client_golang v1.22.0
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.65.0
	github.com/sercand/kuberesolver/v5 v5.1.1
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.62.0
	go.opentelemetry.io/contrib/propagators/b3 v1.37.0
	go.opentelemetry.io/otel v1.37.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.37.0
	go.opentelemetry.io/otel/sdk v1.37.0
	go.opentelemetry.io/otel/trace v1.37.0
	go.opentelemetry.io/proto/otlp v1.7.0
	go.uber.org/mock v0.0.0-00010101000000-000000000000
	golang.org/x/lint v0.0.0-20241112194109-818c5a804067
	golang.org/x/oauth2 v0.30.0
	golang.org/x/sync v0.16.0
	golang.org/x/sys v0.34.0
	google.golang.org/api v0.241.0
	google.golang.org/genproto/googleapis/bytestream v0.0.0-20250707201910-8d1bb00bc6a7
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250707201910-8d1bb00bc6a7
	google.golang.org/grpc v1.73.0
	google.golang.org/grpc/security/advancedtls v1.0.0
	google.golang.org/protobuf v1.36.6
	mvdan.cc/gofumpt v0.8.0
)

require (
	cel.dev/expr v0.24.0 // indirect
	cloud.google.com/go v0.121.3 // indirect
	cloud.google.com/go/auth v0.16.2 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.7.0 // indirect
	cloud.google.com/go/iam v1.5.2 // indirect
	cloud.google.com/go/monitoring v1.24.2 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.29.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.53.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.53.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.11 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.32 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.36 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.36 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.36 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.7.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.30.3 // indirect
	github.com/aws/smithy-go v1.22.4 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20250501225837-2ac532fd4443 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.32.4 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/googleapis/gax-go/v2 v2.15.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.5.0 // indirect
	github.com/zeebo/errs v1.4.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.37.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.62.0 // indirect
	go.opentelemetry.io/otel/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.37.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/crypto v0.40.0 // indirect
	golang.org/x/mod v0.26.0 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/text v0.27.0 // indirect
	golang.org/x/time v0.12.0 // indirect
	golang.org/x/tools v0.35.0 // indirect
	google.golang.org/genproto v0.0.0-20250707201910-8d1bb00bc6a7 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250707201910-8d1bb00bc6a7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	sigs.k8s.io/yaml v1.5.0 // indirect
)
