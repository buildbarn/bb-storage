load("@bazel_gazelle//:deps.bzl", "go_repository")

def go_dependencies():
    go_repository(
        name = "cc_mvdan_gofumpt",
        importpath = "mvdan.cc/gofumpt",
        sum = "h1:0EQ+Z56k8tXjj/6TQD25BFNKQXpCvT0rnansIc7Ug5E=",
        version = "v0.5.0",
    )
    go_repository(
        name = "co_honnef_go_tools",
        importpath = "honnef.co/go/tools",
        sum = "h1:/hemPrYIhOhy8zYrNj+069zDB68us2sMGsfkFJO0iZs=",
        version = "v0.0.0-20190523083050-ea95bdfd59fc",
    )
    go_repository(
        name = "com_github_alecthomas_kingpin_v2",
        importpath = "github.com/alecthomas/kingpin/v2",
        sum = "h1:H0aULhgmSzN8xQ3nX1uxtdlTHYoPLu5AhHxWrKI6ocU=",
        version = "v2.3.2",
    )
    go_repository(
        name = "com_github_alecthomas_units",
        importpath = "github.com/alecthomas/units",
        sum = "h1:s6gZFSlWYmbqAuRjVTiNNhvNRfY2Wxp9nhfyel4rklc=",
        version = "v0.0.0-20211218093645-b94a6e3cc137",
    )
    go_repository(
        name = "com_github_antihax_optional",
        importpath = "github.com/antihax/optional",
        sum = "h1:xK2lYat7ZLaVVcIuj82J8kIro4V6kDe0AUDFboUCwcg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_aohorodnyk_mimeheader",
        importpath = "github.com/aohorodnyk/mimeheader",
        sum = "h1:WCV4NQjtbqnd2N3FT5MEPesan/lfvaLYmt5v4xSaX/M=",
        version = "v0.0.6",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2",
        importpath = "github.com/aws/aws-sdk-go-v2",
        sum = "h1:lV0U8fnhAnPz8YcdmZVV60+tr6CakHzqA6P8T46ExJI=",
        version = "v1.22.2",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_aws_protocol_eventstream",
        importpath = "github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream",
        sum = "h1:hHgLiIrTRtddC0AKcJr5s7i/hLgcpTt+q/FKxf1Zayk=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_config",
        importpath = "github.com/aws/aws-sdk-go-v2/config",
        sum = "h1:kqzEfGGDIrRJpfJckgwuZfFTbU9NB1jZnRcaO9MpOqE=",
        version = "v1.23.0",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_credentials",
        importpath = "github.com/aws/aws-sdk-go-v2/credentials",
        sum = "h1:rKH7khRMxPdD0u3dHecd0Q7NOVw3EUe7AqdkUOkiOGI=",
        version = "v1.15.2",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_feature_ec2_imds",
        importpath = "github.com/aws/aws-sdk-go-v2/feature/ec2/imds",
        sum = "h1:G5KawTAkyHH6WyKQCdHiW4h3PmAXNJpOgwKg3H7sDRE=",
        version = "v1.14.3",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_configsources",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/configsources",
        sum = "h1:AaQsr5vvGR7rmeSWBtTCcw16tT9r51mWijuCQhzLnq8=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_endpoints_v2",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/endpoints/v2",
        sum = "h1:UZx8SXZ0YtzRiALzYAWcjb9Y9hZUR7MBKaBQ5ouOjPs=",
        version = "v2.5.2",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_ini",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/ini",
        sum = "h1:hwZB07/beLiCopuRKF0t+dEHmP39iN4YtDh3X5d3hrg=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_v4a",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/v4a",
        sum = "h1:pyVrNAf7Hwz0u39dLKN5t+n0+K/3rMYKuiOoIum3AsU=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_accept_encoding",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding",
        sum = "h1:CJxo7ZBbaIzmXfV3hjcx36n9V87gJsIUPJflwqEHl3Q=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_checksum",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/checksum",
        sum = "h1:f2LhPofnjcdOQKRtumKjMvIHkfSQ8aH/rwKUDEQ/SB4=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_presigned_url",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/presigned-url",
        sum = "h1:h7j73yuAVVjic8pqswh+L/7r2IHP43QwRyOu6zcCDDE=",
        version = "v1.10.2",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_s3shared",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/s3shared",
        sum = "h1:gbIaOzpXixUpoPK+js/bCBK1QBDXM22SigsnzGZio0U=",
        version = "v1.16.2",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_s3",
        importpath = "github.com/aws/aws-sdk-go-v2/service/s3",
        sum = "h1:o6MCcX1rJW8Y3g+hvg2xpjF6JR6DftuYhfl3Nc1WV9Q=",
        version = "v1.42.1",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_sso",
        importpath = "github.com/aws/aws-sdk-go-v2/service/sso",
        sum = "h1:km+ZNjtLtpXYf42RdaDZnNHm9s7SYAuDGTafy6nd89A=",
        version = "v1.17.1",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_ssooidc",
        importpath = "github.com/aws/aws-sdk-go-v2/service/ssooidc",
        sum = "h1:iRFNqZH4a67IqPvK8xxtyQYnyrlsvwmpHOe9r55ggBA=",
        version = "v1.19.1",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_sts",
        importpath = "github.com/aws/aws-sdk-go-v2/service/sts",
        sum = "h1:txgVXIXWPXyqdiVn92BV6a/rgtpX31HYdsOYj0sVQQQ=",
        version = "v1.25.1",
    )
    go_repository(
        name = "com_github_aws_smithy_go",
        importpath = "github.com/aws/smithy-go",
        sum = "h1:gJZEH/Fqh+RsvlJ1Zt4tVAtV6bKkp3cC+R6FCZMNzik=",
        version = "v1.16.0",
    )
    go_repository(
        name = "com_github_bazelbuild_remote_apis",
        importpath = "github.com/bazelbuild/remote-apis",
        patches = ["@com_github_buildbarn_bb_storage//:patches/com_github_bazelbuild_remote_apis/golang.diff"],
        sum = "h1:TPwjNpCdoO7TcTPPMHEkrrlSwd8g2XVf3qflmnivvsU=",
        version = "v0.0.0-20230822133051-6c32c3b917cc",
    )
    go_repository(
        name = "com_github_benbjohnson_clock",
        importpath = "github.com/benbjohnson/clock",
        sum = "h1:Q92kusRqC1XV2MjkWETPvjJVqKetz1OzxZB7mHJLju8=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_beorn7_perks",
        importpath = "github.com/beorn7/perks",
        sum = "h1:VlbKKnNfV8bJzeqoa4cOKqO6bYr3WgKZxO8Z16+hsOM=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_burntsushi_toml",
        importpath = "github.com/BurntSushi/toml",
        sum = "h1:WXkYYl6Yr3qBf1K79EBnL4mak0OimBfB0XUf9Vl28OQ=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_census_instrumentation_opencensus_proto",
        build_extra_args = ["-exclude=src"],
        importpath = "github.com/census-instrumentation/opencensus-proto",
        sum = "h1:iKLQ0xPNFxR/2hzXZMrBo8f1j86j5WHzznCCQxV/b8g=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_cespare_xxhash_v2",
        importpath = "github.com/cespare/xxhash/v2",
        sum = "h1:DC2CZ1Ep5Y4k3ZQ899DldepgrayRUGE6BBZ/cd9Cj44=",
        version = "v2.2.0",
    )
    go_repository(
        name = "com_github_client9_misspell",
        importpath = "github.com/client9/misspell",
        sum = "h1:ta993UF76GwbvJcIo3Y68y/M3WxlpEHPWIGDkJYwzJI=",
        version = "v0.3.4",
    )
    go_repository(
        name = "com_github_cncf_udpa_go",
        importpath = "github.com/cncf/udpa/go",
        sum = "h1:QQ3GSy+MqSHxm/d8nCtnAiZdYFd45cYZPs8vOOIYKfk=",
        version = "v0.0.0-20220112060539-c52dc94e7fbe",
    )
    go_repository(
        name = "com_github_cncf_xds_go",
        importpath = "github.com/cncf/xds/go",
        sum = "h1:/inchEIKaYC1Akx+H+gqO04wryn5h75LSazbRlnya1k=",
        version = "v0.0.0-20230607035331-e9ce68804cb4",
    )
    go_repository(
        name = "com_github_davecgh_go_spew",
        importpath = "github.com/davecgh/go-spew",
        sum = "h1:vj9j/u1bqnvCEfJOwUhtlOARqs3+rkHYY13jYWTU97c=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_envoyproxy_go_control_plane",
        importpath = "github.com/envoyproxy/go-control-plane",
        sum = "h1:wSUXTlLfiAQRWs2F+p+EKOY9rUyis1MyGqJ2DIk5HpM=",
        version = "v0.11.1",
    )
    go_repository(
        name = "com_github_envoyproxy_protoc_gen_validate",
        importpath = "github.com/envoyproxy/protoc-gen-validate",
        sum = "h1:QkIBuU5k+x7/QXPvPPnWXWlCdaBFApVqftFV6k087DA=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_fatih_color",
        importpath = "github.com/fatih/color",
        sum = "h1:mRhaKNwANqRgUBGKmnI5ZxEk7QXmjQeCcuYFMX2bfcc=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_github_frankban_quicktest",
        importpath = "github.com/frankban/quicktest",
        sum = "h1:g2rn0vABPOOXmZUj+vbmUp0lPoXEMuhTpIluN0XL9UY=",
        version = "v1.14.4",
    )
    go_repository(
        name = "com_github_fxtlabs_primes",
        importpath = "github.com/fxtlabs/primes",
        sum = "h1:HOYnhuVrhAVGKdg3rZapII640so7QfXQmkLkefUN/uM=",
        version = "v0.0.0-20150821004651-dad82d10a449",
    )
    go_repository(
        name = "com_github_go_jose_go_jose_v3",
        importpath = "github.com/go-jose/go-jose/v3",
        sum = "h1:pWmKFVtt+Jl0vBZTIpz/eAKwsm6LkIxDVVbFHKkchhA=",
        version = "v3.0.1",
    )
    go_repository(
        name = "com_github_go_kit_log",
        importpath = "github.com/go-kit/log",
        sum = "h1:MRVx0/zhvdseW+Gza6N9rVzU/IVzaeE1SFI4raAhmBU=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_go_logfmt_logfmt",
        importpath = "github.com/go-logfmt/logfmt",
        sum = "h1:otpy5pqBCBZ1ng9RQ0dPu4PN7ba75Y/aA+UpowDyNVA=",
        version = "v0.5.1",
    )
    go_repository(
        name = "com_github_go_logr_logr",
        importpath = "github.com/go-logr/logr",
        sum = "h1:2y3SDp0ZXuc6/cjLSZ+Q3ir+QB9T/iG5yYRXqsagWSY=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_go_logr_stdr",
        importpath = "github.com/go-logr/stdr",
        sum = "h1:hSWxHoqTgW2S2qGc0LTAI563KZ5YKYRhT3MFKZMbjag=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_go_stack_stack",
        importpath = "github.com/go-stack/stack",
        sum = "h1:5SgMzNM5HxrEjV0ww2lTmX6E2Izsfxas4+YHWRs3Lsk=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_github_gogo_protobuf",
        importpath = "github.com/gogo/protobuf",
        sum = "h1:Ov1cvc58UF3b5XjBnZv7+opcTcQFZebYjWzi34vdm4Q=",
        version = "v1.3.2",
    )
    go_repository(
        name = "com_github_golang_glog",
        importpath = "github.com/golang/glog",
        sum = "h1:DVjP2PbBOzHyzA+dn3WhHIq4NdVu3Q+pvivFICf/7fo=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_golang_groupcache",
        importpath = "github.com/golang/groupcache",
        sum = "h1:oI5xCqsCo564l8iNU+DwB5epxmsaqB+rhGL0m5jtYqE=",
        version = "v0.0.0-20210331224755-41bb18bfe9da",
    )
    go_repository(
        name = "com_github_golang_mock",
        importpath = "github.com/golang/mock",
        sum = "h1:ErTB+efbowRARo13NNdxyJji2egdxLGQhRaY+DUumQc=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_github_golang_protobuf",
        importpath = "github.com/golang/protobuf",
        patches = ["@com_github_buildbarn_bb_storage//:patches/com_github_golang_protobuf/service-registrar.diff"],
        sum = "h1:KhyjKVUg7Usr/dYsdSqoFveMYd5ko72D+zANwlG1mmg=",
        version = "v1.5.3",
    )
    go_repository(
        name = "com_github_golang_snappy",
        importpath = "github.com/golang/snappy",
        sum = "h1:yAGX7huGHXlcLOEtBnF4w7FQwA26wojNCwOYAEhLjQM=",
        version = "v0.0.4",
    )
    go_repository(
        name = "com_github_google_go_cmp",
        importpath = "github.com/google/go-cmp",
        sum = "h1:ofyhxvXcZhMsU5ulbFiLKl/XBFqE1GSq7atu8tAmTRI=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_google_go_jsonnet",
        build_file_generation = "on",
        importpath = "github.com/google/go-jsonnet",
        sum = "h1:WG4TTSARuV7bSm4PMB4ohjxe33IHT5WVTrJSU33uT4g=",
        version = "v0.20.0",
    )
    go_repository(
        name = "com_github_google_go_pkcs11",
        importpath = "github.com/google/go-pkcs11",
        sum = "h1:OF1IPgv+F4NmqmJ98KTjdN97Vs1JxDPB3vbmYzV2dpk=",
        version = "v0.2.1-0.20230907215043-c6f79328ddf9",
    )
    go_repository(
        name = "com_github_google_martian_v3",
        importpath = "github.com/google/martian/v3",
        sum = "h1:IqNFLAmvJOgVlpdEBiQbDc2EwKW77amAycfTuWKdfvw=",
        version = "v3.3.2",
    )
    go_repository(
        name = "com_github_google_s2a_go",
        importpath = "github.com/google/s2a-go",
        sum = "h1:60BLSyTrOV4/haCDW4zb1guZItoSq8foHCXrAnjBo/o=",
        version = "v0.1.7",
    )
    go_repository(
        name = "com_github_google_uuid",
        importpath = "github.com/google/uuid",
        sum = "h1:MtMxsa51/r9yyhkyLsVeVt0B+BGQZzpQiTQ4eHZ8bc4=",
        version = "v1.4.0",
    )
    go_repository(
        name = "com_github_googleapis_enterprise_certificate_proxy",
        importpath = "github.com/googleapis/enterprise-certificate-proxy",
        sum = "h1:Vie5ybvEvT75RniqhfFxPRy3Bf7vr3h0cechB90XaQs=",
        version = "v0.3.2",
    )
    go_repository(
        name = "com_github_googleapis_gax_go_v2",
        build_file_proto_mode = "disable",
        importpath = "github.com/googleapis/gax-go/v2",
        sum = "h1:A+gCJKdRfqXkr+BIRGtZLibNXf0m1f9E4HG56etFpas=",
        version = "v2.12.0",
    )
    go_repository(
        name = "com_github_gorilla_mux",
        importpath = "github.com/gorilla/mux",
        sum = "h1:TuBL49tXwgrFYWhqrNgrUNEY92u81SPhu7sTdzQEiWY=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_middleware",
        importpath = "github.com/grpc-ecosystem/go-grpc-middleware",
        sum = "h1:UH//fgunKIs4JdUbpDl1VZCDaL56wXCB/5+wF6uHfaI=",
        version = "v1.4.0",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_prometheus",
        importpath = "github.com/grpc-ecosystem/go-grpc-prometheus",
        patches = ["@com_github_buildbarn_bb_storage//:patches/com_github_grpc_ecosystem_go_grpc_prometheus/client-metrics-prevent-handled-twice.diff"],
        sum = "h1:Ovs26xHkKqVztRpIrF/92BcuyuQ/YW4NSIpoGtfXNho=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_grpc_gateway_v2",
        importpath = "github.com/grpc-ecosystem/grpc-gateway/v2",
        replace = "github.com/grpc-ecosystem/grpc-gateway/v2",
        sum = "h1:RoziI+96HlQWrbaVhgOOdFYUHtX81pwA6tCgDS9FNRo=",
        version = "v2.16.1",
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath",
        importpath = "github.com/jmespath/go-jmespath",
        sum = "h1:BEgLn5cpjn8UN1mAw4NjwDrS35OdebyEtFe+9YPoQUg=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath_internal_testify",
        importpath = "github.com/jmespath/go-jmespath/internal/testify",
        sum = "h1:shLQSRRSCCPj3f2gpwzGwWFoC7ycTf1rcQZHOlsJ6N8=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_github_jpillora_backoff",
        importpath = "github.com/jpillora/backoff",
        sum = "h1:uvFg412JmmHBHw7iwprIxkPMI+sGQ4kzOWsMeHnm2EA=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_json_iterator_go",
        importpath = "github.com/json-iterator/go",
        sum = "h1:PV8peI4a0ysnczrg+LtxykD8LfKY9ML6u2jnxaEnrnM=",
        version = "v1.1.12",
    )
    go_repository(
        name = "com_github_julienschmidt_httprouter",
        importpath = "github.com/julienschmidt/httprouter",
        sum = "h1:U0609e9tgbseu3rBINet9P48AI/D3oJs4dN7jwJOQ1U=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_kisielk_errcheck",
        importpath = "github.com/kisielk/errcheck",
        sum = "h1:e8esj/e4R+SAOwFwN+n3zr0nYeCyeweozKfO23MvHzY=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_kisielk_gotool",
        importpath = "github.com/kisielk/gotool",
        sum = "h1:AV2c/EiW3KqPNT9ZKl07ehoAGi4C5/01Cfbblndcapg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_klauspost_compress",
        importpath = "github.com/klauspost/compress",
        sum = "h1:RlWWUY/Dr4fL8qk9YG7DTZ7PDgME2V4csBXA8L/ixi4=",
        version = "v1.17.2",
    )
    go_repository(
        name = "com_github_konsorten_go_windows_terminal_sequences",
        importpath = "github.com/konsorten/go-windows-terminal-sequences",
        sum = "h1:mweAR1A6xJ3oS2pRaGiHgQ4OO8tzTaLawm8vnODuwDk=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_kr_pretty",
        importpath = "github.com/kr/pretty",
        sum = "h1:flRD4NNwYAUpkphVc1HcthR4KEIFJ65n8Mw5qdRn3LE=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_kr_pty",
        importpath = "github.com/kr/pty",
        sum = "h1:VkoXIwSboBpnk99O/KFauAEILuNHv5DVFKZMBN/gUgw=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_kr_text",
        importpath = "github.com/kr/text",
        sum = "h1:5Nx0Ya0ZqY2ygV366QzturHI13Jq95ApcVaJBhpS+AY=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_lazybeaver_xorshift",
        importpath = "github.com/lazybeaver/xorshift",
        sum = "h1:TfmftEfB1zJiDTFi3Qw1xlbEbfJPKUhEDC19clfBMb8=",
        version = "v0.0.0-20170702203709-ce511d4823dd",
    )
    go_repository(
        name = "com_github_mattn_go_colorable",
        importpath = "github.com/mattn/go-colorable",
        sum = "h1:c1ghPdyEDarC70ftn0y+A/Ee++9zz8ljHG1b13eJ0s8=",
        version = "v0.1.8",
    )
    go_repository(
        name = "com_github_mattn_go_isatty",
        importpath = "github.com/mattn/go-isatty",
        sum = "h1:wuysRhFDzyxgEmMf5xjvJ2M9dZoWAXNNr5LSBS7uHXY=",
        version = "v0.0.12",
    )
    go_repository(
        name = "com_github_matttproud_golang_protobuf_extensions",
        importpath = "github.com/matttproud/golang_protobuf_extensions",
        sum = "h1:mmDVorXM7PCGKw94cs5zkfA9PSy5pEvNWRP0ET0TIVo=",
        version = "v1.0.4",
    )
    go_repository(
        name = "com_github_matttproud_golang_protobuf_extensions_v2",
        importpath = "github.com/matttproud/golang_protobuf_extensions/v2",
        sum = "h1:jWpvCLoY8Z/e3VKvlsiIGKtc+UG6U5vzxaoagmhXfyg=",
        version = "v2.0.0",
    )
    go_repository(
        name = "com_github_modern_go_concurrent",
        importpath = "github.com/modern-go/concurrent",
        sum = "h1:TRLaZ9cD/w8PVh93nsPXa1VrQ6jlwL5oN8l14QlcNfg=",
        version = "v0.0.0-20180306012644-bacd9c7ef1dd",
    )
    go_repository(
        name = "com_github_modern_go_reflect2",
        importpath = "github.com/modern-go/reflect2",
        sum = "h1:xBagoLtFs94CBntxluKeaWgTMpvLxC4ur3nMaC9Gz0M=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_github_mwitkow_go_conntrack",
        importpath = "github.com/mwitkow/go-conntrack",
        sum = "h1:KUppIJq7/+SVif2QVs3tOP0zanoHgBEVAwHxUSIzRqU=",
        version = "v0.0.0-20190716064945-2f068394615f",
    )
    go_repository(
        name = "com_github_opentracing_opentracing_go",
        importpath = "github.com/opentracing/opentracing-go",
        sum = "h1:pWlfV3Bxv7k65HYwkikxat0+s3pV4bsqf19k25Ur8rU=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_pkg_errors",
        importpath = "github.com/pkg/errors",
        sum = "h1:iURUrRGxPUNPdy5/HRSm+Yj6okJ6UtLINN0Q9M4+h3I=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_github_pmezard_go_difflib",
        importpath = "github.com/pmezard/go-difflib",
        sum = "h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_prometheus_client_golang",
        importpath = "github.com/prometheus/client_golang",
        sum = "h1:rl2sfwZMtSthVU752MqfjQozy7blglC+1SOtjMAMh+Q=",
        version = "v1.17.0",
    )
    go_repository(
        name = "com_github_prometheus_client_model",
        importpath = "github.com/prometheus/client_model",
        sum = "h1:VQw1hfvPvk3Uv6Qf29VrPF32JB6rtbgI6cYPYQjL0Qw=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_prometheus_common",
        importpath = "github.com/prometheus/common",
        sum = "h1:2BGz0eBc2hdMDLnO/8n0jeB3oPrt2D08CekT0lneoxM=",
        version = "v0.45.0",
    )
    go_repository(
        name = "com_github_prometheus_procfs",
        importpath = "github.com/prometheus/procfs",
        sum = "h1:xRC8Iq1yyca5ypa9n1EZnWZkt7dwcoRPQwX/5gwaUuI=",
        version = "v0.11.1",
    )
    go_repository(
        name = "com_github_rogpeppe_fastuuid",
        importpath = "github.com/rogpeppe/fastuuid",
        sum = "h1:Ppwyp6VYCF1nvBTXL3trRso7mXMlRrw9ooo375wvi2s=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_rogpeppe_go_internal",
        importpath = "github.com/rogpeppe/go-internal",
        sum = "h1:TMyTOH3F/DB16zRVcYyreMH6GnZZrwQVAoYjRBZyWFQ=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_github_sergi_go_diff",
        importpath = "github.com/sergi/go-diff",
        sum = "h1:we8PVUC3FE2uYfodKH/nBHMSetSfHDR6scGdBi+erh0=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_sirupsen_logrus",
        importpath = "github.com/sirupsen/logrus",
        sum = "h1:SPIRibHv4MatM3XXNO2BJeFLZwZ2LvZgfQ5+UNI2im4=",
        version = "v1.4.2",
    )
    go_repository(
        name = "com_github_stretchr_objx",
        importpath = "github.com/stretchr/objx",
        sum = "h1:1zr/of2m5FGMsad5YfcqgdqdWrIhu+EBEJRhR1U7z/c=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_stretchr_testify",
        importpath = "github.com/stretchr/testify",
        sum = "h1:CcVxjf3Q8PM0mHUKJCdn+eZZtm5yQwehR5yeSVQQcUk=",
        version = "v1.8.4",
    )
    go_repository(
        name = "com_github_xhit_go_str2duration_v2",
        importpath = "github.com/xhit/go-str2duration/v2",
        sum = "h1:lxklc02Drh6ynqX+DdPyp5pCKLUQpRT8bp8Ydu2Bstc=",
        version = "v2.1.0",
    )
    go_repository(
        name = "com_github_yuin_goldmark",
        importpath = "github.com/yuin/goldmark",
        sum = "h1:fVcFKWvrslecOb/tg+Cc05dkeYx540o0FuFt3nUVDoE=",
        version = "v1.4.13",
    )
    go_repository(
        name = "com_google_cloud_go",
        importpath = "cloud.google.com/go",
        sum = "h1:e7ITSqGFFk4rbz/JFIqZh3G4VEHguhAL4BQcFlWtU68=",
        version = "v0.110.9",
    )
    go_repository(
        name = "com_google_cloud_go_accessapproval",
        importpath = "cloud.google.com/go/accessapproval",
        sum = "h1:MOfZ2ncDO+nctNmOYqN8kCb8kHkVJU1nqGNGDa7qGQk=",
        version = "v1.7.3",
    )
    go_repository(
        name = "com_google_cloud_go_accesscontextmanager",
        importpath = "cloud.google.com/go/accesscontextmanager",
        sum = "h1:4vUQZ3OqH3YWo18AgQgvked8NSszsZQfURsYurRHoZI=",
        version = "v1.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_aiplatform",
        importpath = "cloud.google.com/go/aiplatform",
        sum = "h1:DhbEMbUZTbsIQBhErvo5GlQCymVM50aoP3UDcrnxuOo=",
        version = "v1.51.2",
    )
    go_repository(
        name = "com_google_cloud_go_analytics",
        importpath = "cloud.google.com/go/analytics",
        sum = "h1:ccW5bYKOrgXAbvIT/mxqk2rou7CfrUdl5G5bJDmDeVA=",
        version = "v0.21.5",
    )
    go_repository(
        name = "com_google_cloud_go_apigateway",
        importpath = "cloud.google.com/go/apigateway",
        sum = "h1:8zRrVIaGC5dQeq3lBUn7Kchea8ao0bUILXeY8kBzF3M=",
        version = "v1.6.3",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeconnect",
        importpath = "cloud.google.com/go/apigeeconnect",
        sum = "h1:piXKIidKtP/FQ44dnYXGF+WSDsegdPmj/EKk6Hc3sR4=",
        version = "v1.6.3",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeregistry",
        importpath = "cloud.google.com/go/apigeeregistry",
        sum = "h1:GuexCawKvjXeeLgBYJoS1m7TCWvbYrmb9KNZjf5EdxA=",
        version = "v0.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_appengine",
        importpath = "cloud.google.com/go/appengine",
        sum = "h1:7hfqb1kKeB/jAyzOiBB00cQ71GYYLal85VH+uJhE5kc=",
        version = "v1.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_area120",
        importpath = "cloud.google.com/go/area120",
        sum = "h1:wcVVp8EmTVVXJNU8QHDOZOHC5iJYNoU0jw8daZyvPQg=",
        version = "v0.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_artifactregistry",
        importpath = "cloud.google.com/go/artifactregistry",
        sum = "h1:rE++Rall/sWmbWkkQEfNoXC7GkCP7hqqd7NeDNEfGqI=",
        version = "v1.14.4",
    )
    go_repository(
        name = "com_google_cloud_go_asset",
        importpath = "cloud.google.com/go/asset",
        sum = "h1:B3OhouMFZNnUpB26Ex1izZ3OBh9R3iAGWO8raLOZ0jA=",
        version = "v1.15.2",
    )
    go_repository(
        name = "com_google_cloud_go_assuredworkloads",
        importpath = "cloud.google.com/go/assuredworkloads",
        sum = "h1:1eef9dFe2ylYw/6Nv6vP2t4xEeUHGIZDKRBwFbsiQUs=",
        version = "v1.11.3",
    )
    go_repository(
        name = "com_google_cloud_go_automl",
        importpath = "cloud.google.com/go/automl",
        sum = "h1:8gL+S9PpsYrqa9v7PDGMWEb42EWLGlxjdjMOB7/wU1c=",
        version = "v1.13.3",
    )
    go_repository(
        name = "com_google_cloud_go_baremetalsolution",
        importpath = "cloud.google.com/go/baremetalsolution",
        sum = "h1:nS60vlA7w2GTXpQ7a6y5z4jjJF46kshx3G2V99WLsbA=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_google_cloud_go_batch",
        importpath = "cloud.google.com/go/batch",
        sum = "h1:wa8ZC1wMJLqnwWsGiVsntBIEYcIP+HUyAcKc0DhoDE4=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_beyondcorp",
        importpath = "cloud.google.com/go/beyondcorp",
        sum = "h1:/TrG41OifURpA7weWrG9CROcBapc0exDiDaL9ZgplXE=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_google_cloud_go_bigquery",
        importpath = "cloud.google.com/go/bigquery",
        sum = "h1:LHIc9E7Kw+ftFpQFKzZYBB88IAFz7qONawXXx0F3QBo=",
        version = "v1.56.0",
    )
    go_repository(
        name = "com_google_cloud_go_billing",
        importpath = "cloud.google.com/go/billing",
        sum = "h1:PIpLB0uqYRKyzbPZNQV+FxvBcKtZH+Io9eK/lC8bRSE=",
        version = "v1.17.3",
    )
    go_repository(
        name = "com_google_cloud_go_binaryauthorization",
        importpath = "cloud.google.com/go/binaryauthorization",
        sum = "h1:pSb8eiGofANJbRNTwQDiLJhMjng4tvFEVDEbaf2UWwM=",
        version = "v1.7.2",
    )
    go_repository(
        name = "com_google_cloud_go_certificatemanager",
        importpath = "cloud.google.com/go/certificatemanager",
        sum = "h1:dOD0bZPTwtFo/h8CwCBpu/m3B4ugm2GFhcym/tuoaJg=",
        version = "v1.7.3",
    )
    go_repository(
        name = "com_google_cloud_go_channel",
        importpath = "cloud.google.com/go/channel",
        sum = "h1:LcaJyv6riQA39bvkPigOJvvNJM7Ls5x0hjZAqcuxq60=",
        version = "v1.17.2",
    )
    go_repository(
        name = "com_google_cloud_go_cloudbuild",
        importpath = "cloud.google.com/go/cloudbuild",
        sum = "h1:Go7wFI+P5QyuSt1sZtb8wx+M+MGjyfR+xgOzjuZMDF8=",
        version = "v1.14.2",
    )
    go_repository(
        name = "com_google_cloud_go_clouddms",
        importpath = "cloud.google.com/go/clouddms",
        sum = "h1:qxyV8j7ckzzt5gcX1u5qkteKBDMd9Q29TPs7uZVV6bA=",
        version = "v1.7.2",
    )
    go_repository(
        name = "com_google_cloud_go_cloudtasks",
        importpath = "cloud.google.com/go/cloudtasks",
        sum = "h1:ajm0DZTTaGKeawbwuFXZfPC7IIeLg6eYJFH125i18tE=",
        version = "v1.12.3",
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        importpath = "cloud.google.com/go/compute",
        sum = "h1:nWEMDhgbBkBJjfpVySqU4jgWdc22PLR0o4vEexZHers=",
        version = "v1.23.2",
    )
    go_repository(
        name = "com_google_cloud_go_compute_metadata",
        importpath = "cloud.google.com/go/compute/metadata",
        sum = "h1:mg4jlk7mCAj6xXp9UJ4fjI9VUI5rubuGBW5aJ7UnBMY=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_google_cloud_go_contactcenterinsights",
        importpath = "cloud.google.com/go/contactcenterinsights",
        sum = "h1:xTLvUYWMwNdQeAr9FECdenht43In0s/KAyFHSuKuZR4=",
        version = "v1.11.2",
    )
    go_repository(
        name = "com_google_cloud_go_container",
        importpath = "cloud.google.com/go/container",
        sum = "h1:vNQsufqH93Df+U1kNNgjQ6UIQ1v4FUdQ9QHdDiiDuss=",
        version = "v1.26.2",
    )
    go_repository(
        name = "com_google_cloud_go_containeranalysis",
        importpath = "cloud.google.com/go/containeranalysis",
        sum = "h1:AKyhqlO1Cjdrp7Ht0C3uQALliri2RegVRpEZseOKwWo=",
        version = "v0.11.2",
    )
    go_repository(
        name = "com_google_cloud_go_datacatalog",
        importpath = "cloud.google.com/go/datacatalog",
        sum = "h1:4ydlNOtwjkdXjXWd+SkUBh+DyVmM/bJKiktAHwqaEeU=",
        version = "v1.18.2",
    )
    go_repository(
        name = "com_google_cloud_go_dataflow",
        importpath = "cloud.google.com/go/dataflow",
        sum = "h1:v+wjNFEugYBYaowmqJuHK1piqNCx9pGvUWPEhspRFEw=",
        version = "v0.9.3",
    )
    go_repository(
        name = "com_google_cloud_go_dataform",
        importpath = "cloud.google.com/go/dataform",
        sum = "h1:jhr9PQtaH59nfZbI7tP0jjIl9SlfnRgteAQHD+ZT2xA=",
        version = "v0.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_datafusion",
        importpath = "cloud.google.com/go/datafusion",
        sum = "h1:v5HtQxvFLVHgbJXUdd4zbNZcYgknRUEW9ukOYpwf6d0=",
        version = "v1.7.3",
    )
    go_repository(
        name = "com_google_cloud_go_datalabeling",
        importpath = "cloud.google.com/go/datalabeling",
        sum = "h1:wYqhVo4f5akRW2yZfGQ56fUCi8FCgYzKh1UvBeg7edE=",
        version = "v0.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_dataplex",
        importpath = "cloud.google.com/go/dataplex",
        sum = "h1:D+UlBA3Z+k/Z93a1Wh3uZrUbc4aX7c7ifF/m/s5H6fQ=",
        version = "v1.10.2",
    )
    go_repository(
        name = "com_google_cloud_go_dataproc_v2",
        importpath = "cloud.google.com/go/dataproc/v2",
        sum = "h1:XRnxqa08/P2LpXTB+OMmPAfhT7GGyftgslKvzv330gM=",
        version = "v2.2.2",
    )
    go_repository(
        name = "com_google_cloud_go_dataqna",
        importpath = "cloud.google.com/go/dataqna",
        sum = "h1:Nr5t9MoTDxlOc7lVsydsIZ31ZdxOznCyKf5qFCiPu4o=",
        version = "v0.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_datastore",
        importpath = "cloud.google.com/go/datastore",
        sum = "h1:0P9WcsQeTWjuD1H14JIY7XQscIPQ4Laje8ti96IC5vg=",
        version = "v1.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_datastream",
        importpath = "cloud.google.com/go/datastream",
        sum = "h1:94RSCSWUXpbXvTrISGKZWbJqPTTSvMxErCZiHrEspEM=",
        version = "v1.10.2",
    )
    go_repository(
        name = "com_google_cloud_go_deploy",
        importpath = "cloud.google.com/go/deploy",
        sum = "h1:LoaXLE32tVW/ULfKP8iX3USJZopWUTjVHiIGc/CVcWI=",
        version = "v1.14.1",
    )
    go_repository(
        name = "com_google_cloud_go_dialogflow",
        importpath = "cloud.google.com/go/dialogflow",
        sum = "h1:y2I9aHjFrszP2mAQNFCkfwzyli1avPiw/OQt1K3YV8Q=",
        version = "v1.44.2",
    )
    go_repository(
        name = "com_google_cloud_go_dlp",
        importpath = "cloud.google.com/go/dlp",
        sum = "h1:UWS2DOpyUuK/ig3ZWzJ0IcZRYsRcRkgD1NSaoSU1upw=",
        version = "v1.10.3",
    )
    go_repository(
        name = "com_google_cloud_go_documentai",
        importpath = "cloud.google.com/go/documentai",
        sum = "h1:AQ9/q6mFBpHmWx7k1fB+S1ut8mv03XUo9sdOuEWXxl8=",
        version = "v1.23.4",
    )
    go_repository(
        name = "com_google_cloud_go_domains",
        importpath = "cloud.google.com/go/domains",
        sum = "h1:/NaEe9O1vhHt+j/VDPNJt8oDgGthsY+hbjbkBBOWorw=",
        version = "v0.9.3",
    )
    go_repository(
        name = "com_google_cloud_go_edgecontainer",
        importpath = "cloud.google.com/go/edgecontainer",
        sum = "h1:cpoLa8/f2xPQFfoXRztRMJVVsQ/pxY+fra1QGQjxjyw=",
        version = "v1.1.3",
    )
    go_repository(
        name = "com_google_cloud_go_errorreporting",
        importpath = "cloud.google.com/go/errorreporting",
        sum = "h1:kj1XEWMu8P0qlLhm3FwcaFsUvXChV/OraZwA70trRR0=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_essentialcontacts",
        importpath = "cloud.google.com/go/essentialcontacts",
        sum = "h1:kCxxUXP61/36M9KjiwvwSDAjtpjiv2apmIbBRdBHNXg=",
        version = "v1.6.4",
    )
    go_repository(
        name = "com_google_cloud_go_eventarc",
        importpath = "cloud.google.com/go/eventarc",
        sum = "h1:8LX0xmWk8qj4BZJ5rwQosVW/icozTsMxUimgz2Hlw5s=",
        version = "v1.13.2",
    )
    go_repository(
        name = "com_google_cloud_go_filestore",
        importpath = "cloud.google.com/go/filestore",
        sum = "h1:Rx1EFBco717fTbeQLhAgEdPStqAzlKywMx37SvTOFXI=",
        version = "v1.7.3",
    )
    go_repository(
        name = "com_google_cloud_go_firestore",
        importpath = "cloud.google.com/go/firestore",
        sum = "h1:8aLcKnMPoldYU3YHgu4t2exrKhLQkqaXAGqT0ljrFVw=",
        version = "v1.14.0",
    )
    go_repository(
        name = "com_google_cloud_go_functions",
        importpath = "cloud.google.com/go/functions",
        sum = "h1:5bAA0oK69rdOzSyJGkEod90R9zpok2CRPi1du0gwFtc=",
        version = "v1.15.3",
    )
    go_repository(
        name = "com_google_cloud_go_gkebackup",
        importpath = "cloud.google.com/go/gkebackup",
        sum = "h1:B1QCKBePacacXFflG3Sc1MP4zWkeN1ZvfAT0AEAEgIc=",
        version = "v1.3.3",
    )
    go_repository(
        name = "com_google_cloud_go_gkeconnect",
        importpath = "cloud.google.com/go/gkeconnect",
        sum = "h1:Qex3VdGKPM9cRbEIe7FAho8CAMQOx4u+m1pwRU515jY=",
        version = "v0.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_gkehub",
        importpath = "cloud.google.com/go/gkehub",
        sum = "h1:b7/eKuEEfeQeRIAz//v6Y3ZjNAYi81Eay/a8kqaYSKI=",
        version = "v0.14.3",
    )
    go_repository(
        name = "com_google_cloud_go_gkemulticloud",
        importpath = "cloud.google.com/go/gkemulticloud",
        sum = "h1:eBkfrcLHG+uaaVVbKMKS0qA1M1yBU3pdiHn1rZUCv0s=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_google_cloud_go_gsuiteaddons",
        importpath = "cloud.google.com/go/gsuiteaddons",
        sum = "h1:fjt+72G2ZavxZ4JROTjrvjV6OJs/fHr9Uo+aOGDcH20=",
        version = "v1.6.3",
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        importpath = "cloud.google.com/go/iam",
        sum = "h1:K6n/GZHFTtEoKT5aUG3l9diPi0VduZNQ1PfdnpkkIFk=",
        version = "v1.1.4",
    )
    go_repository(
        name = "com_google_cloud_go_iap",
        importpath = "cloud.google.com/go/iap",
        sum = "h1:vuyDICrMq8z6flZaptQWOxN+XBHtGKWKYlAir8LmTgk=",
        version = "v1.9.2",
    )
    go_repository(
        name = "com_google_cloud_go_ids",
        importpath = "cloud.google.com/go/ids",
        sum = "h1:ZgT7bJIu+7toDJNjX4Uiao/9+82T6ZDGySoMxbvdZcY=",
        version = "v1.4.3",
    )
    go_repository(
        name = "com_google_cloud_go_iot",
        importpath = "cloud.google.com/go/iot",
        sum = "h1:Cbn4wsDXgIwqAAOYIyto4e6aXJDhlZiqf2wO+9h8pEg=",
        version = "v1.7.3",
    )
    go_repository(
        name = "com_google_cloud_go_kms",
        importpath = "cloud.google.com/go/kms",
        sum = "h1:gEZzC54ZBI+aeW8/jg9tgz9KR4Aa+WEDPbdGIV3iJ7A=",
        version = "v1.15.4",
    )
    go_repository(
        name = "com_google_cloud_go_language",
        importpath = "cloud.google.com/go/language",
        sum = "h1:7TnnZKy3sspQ3F1mzwEv3eaptTwGrReVXEo8Oozi10w=",
        version = "v1.12.1",
    )
    go_repository(
        name = "com_google_cloud_go_lifesciences",
        importpath = "cloud.google.com/go/lifesciences",
        sum = "h1:QcPrlyo/F7IXJXk8CkF4yGEwX4CTLAKUE/My4l3KF/8=",
        version = "v0.9.3",
    )
    go_repository(
        name = "com_google_cloud_go_logging",
        importpath = "cloud.google.com/go/logging",
        sum = "h1:26skQWPeYhvIasWKm48+Eq7oUqdcdbwsCVwz5Ys0FvU=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_longrunning",
        importpath = "cloud.google.com/go/longrunning",
        sum = "h1:w8xEcbZodnA2BbW6sVirkkoC+1gP8wS57EUUgGS0GVg=",
        version = "v0.5.4",
    )
    go_repository(
        name = "com_google_cloud_go_managedidentities",
        importpath = "cloud.google.com/go/managedidentities",
        sum = "h1:hfWT2pidjP19mYJxTWMbfj8/wj9OZNUtB/oZ3HW0K8g=",
        version = "v1.6.3",
    )
    go_repository(
        name = "com_google_cloud_go_maps",
        importpath = "cloud.google.com/go/maps",
        sum = "h1:j5AJNj9mbPTwqrqFmAHzMY3UX+Eddj/jC9xu7qFMVhk=",
        version = "v1.5.1",
    )
    go_repository(
        name = "com_google_cloud_go_mediatranslation",
        importpath = "cloud.google.com/go/mediatranslation",
        sum = "h1:3B75VHLlp4M+W10NHEOAl30q7ZSd0D/i8LqRQqxqMM0=",
        version = "v0.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_memcache",
        importpath = "cloud.google.com/go/memcache",
        sum = "h1:yD1ZDTXP7YfT5lBLRNioqZ427iaHaoqW5aMKXtM0acQ=",
        version = "v1.10.3",
    )
    go_repository(
        name = "com_google_cloud_go_metastore",
        importpath = "cloud.google.com/go/metastore",
        sum = "h1:ATz8sWfuHmlqAkPpWGRXQzZFUsgbDkAhZOyNlce83vA=",
        version = "v1.13.2",
    )
    go_repository(
        name = "com_google_cloud_go_monitoring",
        importpath = "cloud.google.com/go/monitoring",
        sum = "h1:gx7BDZcoRqX5DfuJzw9LdhVjEkqCLmDXScdnrmIy9ik=",
        version = "v1.16.2",
    )
    go_repository(
        name = "com_google_cloud_go_networkconnectivity",
        importpath = "cloud.google.com/go/networkconnectivity",
        sum = "h1:tbzauwO86MlZ7+7OmxA0+PnEEgK2AXo9lGrUIL0++qc=",
        version = "v1.14.2",
    )
    go_repository(
        name = "com_google_cloud_go_networkmanagement",
        importpath = "cloud.google.com/go/networkmanagement",
        sum = "h1:v5RWynWBSX91pDnGp/7+5ME0/wLsjIzrC6vlICE/YPs=",
        version = "v1.9.2",
    )
    go_repository(
        name = "com_google_cloud_go_networksecurity",
        importpath = "cloud.google.com/go/networksecurity",
        sum = "h1:t1QESL7xdq/fIoPHrduMWsMDYnM092UUuuiDvnXgrsk=",
        version = "v0.9.3",
    )
    go_repository(
        name = "com_google_cloud_go_notebooks",
        importpath = "cloud.google.com/go/notebooks",
        sum = "h1:fW/P76U437Eyzl1qEwqQSPeToiMB1LM1QyRGR0FpsG4=",
        version = "v1.11.1",
    )
    go_repository(
        name = "com_google_cloud_go_optimization",
        importpath = "cloud.google.com/go/optimization",
        sum = "h1:sY8QjN37zKQfXCZaUSfGvpVGqyFXO2/vNvrhXMj28eI=",
        version = "v1.6.1",
    )
    go_repository(
        name = "com_google_cloud_go_orchestration",
        importpath = "cloud.google.com/go/orchestration",
        sum = "h1:+oaZtNXbCE9QBV/hCUo8Kf5VFILMfg2KR1n6oh8LB/o=",
        version = "v1.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_orgpolicy",
        importpath = "cloud.google.com/go/orgpolicy",
        sum = "h1:oqXeClkeznFGEd/rcu4T0WYOTh5ec6OkkXskaShD4hM=",
        version = "v1.11.3",
    )
    go_repository(
        name = "com_google_cloud_go_osconfig",
        importpath = "cloud.google.com/go/osconfig",
        sum = "h1:PumxpZU71EpygOx0zkrx2HxlMvpyUmENB4e5nQc3+KM=",
        version = "v1.12.3",
    )
    go_repository(
        name = "com_google_cloud_go_oslogin",
        importpath = "cloud.google.com/go/oslogin",
        sum = "h1:kAQFiAT/+jefVnYYNT4+PxdHrZTFSgeJoE6ykhaGlNo=",
        version = "v1.12.1",
    )
    go_repository(
        name = "com_google_cloud_go_phishingprotection",
        importpath = "cloud.google.com/go/phishingprotection",
        sum = "h1:ZVYpZWmg8leUAp4y5XzhxropUfkcAmwOwsmr4i1D1Mc=",
        version = "v0.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_policytroubleshooter",
        importpath = "cloud.google.com/go/policytroubleshooter",
        sum = "h1:TboGw1ua94FjA/Zgj9Gl4Ksgs/TyasQmAkW9w+vyje4=",
        version = "v1.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_privatecatalog",
        importpath = "cloud.google.com/go/privatecatalog",
        sum = "h1:xSUuUCfA5fo59N1hsQmvzgNispoORTUvqN27XXyWFWg=",
        version = "v0.9.3",
    )
    go_repository(
        name = "com_google_cloud_go_pubsub",
        importpath = "cloud.google.com/go/pubsub",
        sum = "h1:6SPCPvWav64tj0sVX/+npCBKhUi/UjJehy9op/V3p2g=",
        version = "v1.33.0",
    )
    go_repository(
        name = "com_google_cloud_go_pubsublite",
        importpath = "cloud.google.com/go/pubsublite",
        sum = "h1:pX+idpWMIH30/K7c0epN6V703xpIcMXWRjKJsz0tYGY=",
        version = "v1.8.1",
    )
    go_repository(
        name = "com_google_cloud_go_recaptchaenterprise_v2",
        importpath = "cloud.google.com/go/recaptchaenterprise/v2",
        sum = "h1:vbPKIAPiFxHG7uNXZmuivxbox17up/Tav5Y+Z+KA008=",
        version = "v2.8.2",
    )
    go_repository(
        name = "com_google_cloud_go_recommendationengine",
        importpath = "cloud.google.com/go/recommendationengine",
        sum = "h1:A58sxjTGDJC+a2zkCzbYdLtPG1OQ7WijwCv7ZsTKIP8=",
        version = "v0.8.3",
    )
    go_repository(
        name = "com_google_cloud_go_recommender",
        importpath = "cloud.google.com/go/recommender",
        sum = "h1:/uQYJ01mKw0cig+DeMTQ+JuG0BEwVfxVh2gGIG93UEw=",
        version = "v1.11.2",
    )
    go_repository(
        name = "com_google_cloud_go_redis",
        importpath = "cloud.google.com/go/redis",
        sum = "h1:+SH8u5Mrgo8EQG/0/C9fwBnzjYS4CQIm+ZK0nlFtg5I=",
        version = "v1.13.3",
    )
    go_repository(
        name = "com_google_cloud_go_resourcemanager",
        importpath = "cloud.google.com/go/resourcemanager",
        sum = "h1:jftfMUTNJLM9V+4wvMQfKfJfQVWcPJifWsMQnJyL3Jk=",
        version = "v1.9.3",
    )
    go_repository(
        name = "com_google_cloud_go_resourcesettings",
        importpath = "cloud.google.com/go/resourcesettings",
        sum = "h1:T3HzhoHhk0VdEdXm5p+KKoC7jh9C4Xpe3BnNf64UfKU=",
        version = "v1.6.3",
    )
    go_repository(
        name = "com_google_cloud_go_retail",
        importpath = "cloud.google.com/go/retail",
        sum = "h1:pFW+3HJI5qHdE5yprmciAsM6G6cp0h0RH8FELX8K7sw=",
        version = "v1.14.3",
    )
    go_repository(
        name = "com_google_cloud_go_run",
        importpath = "cloud.google.com/go/run",
        sum = "h1:AiA5O0YTCTkLQaJu+HFAS0W/jktmiNydOUgg/d0xQrs=",
        version = "v1.3.2",
    )
    go_repository(
        name = "com_google_cloud_go_scheduler",
        importpath = "cloud.google.com/go/scheduler",
        sum = "h1:1UwFQBqNlwnfpjZbPYitdV/GURiVRg4gbhRnKtL5ZRg=",
        version = "v1.10.3",
    )
    go_repository(
        name = "com_google_cloud_go_secretmanager",
        importpath = "cloud.google.com/go/secretmanager",
        sum = "h1:Xk7NFAUuW9q+eX4AxwS/dDuhlNWQ1MmfxQpsfbgRsEg=",
        version = "v1.11.3",
    )
    go_repository(
        name = "com_google_cloud_go_security",
        importpath = "cloud.google.com/go/security",
        sum = "h1:RXYyXEtudhyqqNZNzVZWuDW5BkSCfqo2OOgOKcxg1Ho=",
        version = "v1.15.3",
    )
    go_repository(
        name = "com_google_cloud_go_securitycenter",
        importpath = "cloud.google.com/go/securitycenter",
        sum = "h1:oJRemSVC122SKmPV1aEkMOTnFHsnHwsQtXBBV55AxoU=",
        version = "v1.24.1",
    )
    go_repository(
        name = "com_google_cloud_go_servicedirectory",
        importpath = "cloud.google.com/go/servicedirectory",
        sum = "h1:vdo3qTQWhCB408ckNYqS29i8btBSiVDSWDYSLKc0iF8=",
        version = "v1.11.2",
    )
    go_repository(
        name = "com_google_cloud_go_shell",
        importpath = "cloud.google.com/go/shell",
        sum = "h1:qujhSLWp8XUsFMp+RgGn2w+BBrQ7AnymGd+6TRaJ4HM=",
        version = "v1.7.3",
    )
    go_repository(
        name = "com_google_cloud_go_spanner",
        importpath = "cloud.google.com/go/spanner",
        sum = "h1:l3exhhsVMKsx1E7Xd1QajYSvHmI1KZoWPW5tRxIIdvQ=",
        version = "v1.51.0",
    )
    go_repository(
        name = "com_google_cloud_go_speech",
        importpath = "cloud.google.com/go/speech",
        sum = "h1:BvK9bjSW3Ut1hNvYXn2nlu/oGE+MUyjfIEZcI3kgbM8=",
        version = "v1.19.2",
    )
    go_repository(
        name = "com_google_cloud_go_storage",
        importpath = "cloud.google.com/go/storage",
        sum = "h1:B59ahL//eDfx2IIKFBeT5Atm9wnNmj3+8xG/W4WB//w=",
        version = "v1.35.1",
    )
    go_repository(
        name = "com_google_cloud_go_storagetransfer",
        importpath = "cloud.google.com/go/storagetransfer",
        sum = "h1:rlcXNHgZY/1OIsZHAVDUu2roTs1/3UiVUdUSGnjw4aY=",
        version = "v1.10.2",
    )
    go_repository(
        name = "com_google_cloud_go_talent",
        importpath = "cloud.google.com/go/talent",
        sum = "h1:yLBx78BoR5/sOCE0O6XqzaqL4KLM0+2BEdQBua9Zy1c=",
        version = "v1.6.4",
    )
    go_repository(
        name = "com_google_cloud_go_texttospeech",
        importpath = "cloud.google.com/go/texttospeech",
        sum = "h1:EgMsIlyacLJPrqFZZrXK1mUztyM4cp8wR4Ia1H51a9s=",
        version = "v1.7.3",
    )
    go_repository(
        name = "com_google_cloud_go_tpu",
        importpath = "cloud.google.com/go/tpu",
        sum = "h1:euDiTIKKnQ3nG1J0KCafMJffxRdPNWQsnRvGJ7lZWSk=",
        version = "v1.6.3",
    )
    go_repository(
        name = "com_google_cloud_go_trace",
        importpath = "cloud.google.com/go/trace",
        sum = "h1:yiQ2EdegdiZDPwNPoUqGYAMd0wNValT/NRsC1XGyBJ0=",
        version = "v1.10.3",
    )
    go_repository(
        name = "com_google_cloud_go_translate",
        importpath = "cloud.google.com/go/translate",
        sum = "h1:liPk0EElBpafg4VAlZQwbg786X8/nnboOC4TmAN5sF8=",
        version = "v1.9.2",
    )
    go_repository(
        name = "com_google_cloud_go_video",
        importpath = "cloud.google.com/go/video",
        sum = "h1:paeXmflm8qH81wzND2V6BrKkS3GuvLuSrktHpdh+fr0=",
        version = "v1.20.2",
    )
    go_repository(
        name = "com_google_cloud_go_videointelligence",
        importpath = "cloud.google.com/go/videointelligence",
        sum = "h1:PS8HSjrQh6A1XTKfGz83vLfXhHyGGEiOTgmh2WksdIM=",
        version = "v1.11.3",
    )
    go_repository(
        name = "com_google_cloud_go_vision_v2",
        importpath = "cloud.google.com/go/vision/v2",
        sum = "h1:lTPDW1JogEcR7NSobxlpGAIN1wc2kvUrEfsS/32nm90=",
        version = "v2.7.4",
    )
    go_repository(
        name = "com_google_cloud_go_vmmigration",
        importpath = "cloud.google.com/go/vmmigration",
        sum = "h1:2RIVjbHdVPXw8dhFLAkbU+DGnR/Egiv+zhb0CmSF58U=",
        version = "v1.7.3",
    )
    go_repository(
        name = "com_google_cloud_go_vmwareengine",
        importpath = "cloud.google.com/go/vmwareengine",
        sum = "h1:JU4HKWkJSfYP9eU3q5VVnmyTjiZSqFC6ZXN752ebZuQ=",
        version = "v1.0.2",
    )
    go_repository(
        name = "com_google_cloud_go_vpcaccess",
        importpath = "cloud.google.com/go/vpcaccess",
        sum = "h1:d/vObL8itQ5DmG+fh3wExKCl4zuwbInSQOuysfvgShk=",
        version = "v1.7.3",
    )
    go_repository(
        name = "com_google_cloud_go_webrisk",
        importpath = "cloud.google.com/go/webrisk",
        sum = "h1:9yteSCrAhlZFvu+zb/SlpCKFwij69Il1aIuhk8vq0sg=",
        version = "v1.9.3",
    )
    go_repository(
        name = "com_google_cloud_go_websecurityscanner",
        importpath = "cloud.google.com/go/websecurityscanner",
        sum = "h1:nIo6koOYifrmNUsuXYzlO39oRrV8aM7NdjKjXxL/xvE=",
        version = "v1.6.3",
    )
    go_repository(
        name = "com_google_cloud_go_workflows",
        importpath = "cloud.google.com/go/workflows",
        sum = "h1:dDTsUCgUQbkEFviNMgRV0THXa9VWBNS+35Ru1uadEUw=",
        version = "v1.12.2",
    )
    go_repository(
        name = "in_gopkg_check_v1",
        importpath = "gopkg.in/check.v1",
        sum = "h1:Hei/4ADfdWqJk1ZMxUNpqntNwaWcugrBjAiHlqqRiVk=",
        version = "v1.0.0-20201130134442-10cb98267c6c",
    )
    go_repository(
        name = "in_gopkg_yaml_v2",
        importpath = "gopkg.in/yaml.v2",
        sum = "h1:D8xgwECY7CYvx+Y2n4sBz93Jn9JRvxdiyyo8CTfuKaY=",
        version = "v2.4.0",
    )
    go_repository(
        name = "in_gopkg_yaml_v3",
        importpath = "gopkg.in/yaml.v3",
        sum = "h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=",
        version = "v3.0.1",
    )
    go_repository(
        name = "io_k8s_sigs_yaml",
        importpath = "sigs.k8s.io/yaml",
        sum = "h1:4A07+ZFc2wgJwo8YNlQpr1rVlgUDlxXHhPJciaPY5gs=",
        version = "v1.1.0",
    )
    go_repository(
        name = "io_opencensus_go",
        importpath = "go.opencensus.io",
        sum = "h1:y73uSU6J157QMP2kn2r30vwW1A2W2WFwSCGnAVxeaD0=",
        version = "v0.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc",
        importpath = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
        sum = "h1:PzIubN4/sjByhDRHLviCjJuweBXWFZWhghjg7cS28+M=",
        version = "v0.46.0",
    )
    go_repository(
        name = "io_opentelemetry_go_contrib_propagators_b3",
        importpath = "go.opentelemetry.io/contrib/propagators/b3",
        sum = "h1:uGdgDPNzwQWRwCXJgw/7h29JaRqcq9B87Iv4hJDKAZw=",
        version = "v1.21.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel",
        build_file_proto_mode = "disable",
        importpath = "go.opentelemetry.io/otel",
        sum = "h1:vsb/ggIY+hUjD/zCAQHpzTmndPqv/ml2ArbsbfBYTAc=",
        version = "v1.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_jaeger",
        importpath = "go.opentelemetry.io/otel/exporters/jaeger",
        sum = "h1:D7UpUy2Xc2wsi1Ras6V40q806WM07rqoCWzXu7Sqy+4=",
        version = "v1.17.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace",
        sum = "h1:DeFD0VgTZ+Cj6hxravYYZE2W4GlneVH81iAOPjZkzk8=",
        version = "v1.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_metric",
        importpath = "go.opentelemetry.io/otel/metric",
        sum = "h1:ZlrO8Hu9+GAhnepmRGhSU7/VkpjrNowxRN9GyKR4wzA=",
        version = "v1.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk",
        importpath = "go.opentelemetry.io/otel/sdk",
        sum = "h1:5Jf6imeFZlZtKv9Qbo6qt2ZkmWtdWx/wzcCbNUlAWGM=",
        version = "v1.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_otel_trace",
        importpath = "go.opentelemetry.io/otel/trace",
        sum = "h1:+yxVAPZPbQhbC3OfAkeIVTky6iTFpcr4SiY9om7mXSQ=",
        version = "v1.20.0",
    )
    go_repository(
        name = "io_opentelemetry_go_proto_otlp",
        importpath = "go.opentelemetry.io/proto/otlp",
        sum = "h1:T0TX0tmXU8a3CbNXzEKGeU5mIVOdf0oykP+u2lIVU/I=",
        version = "v1.0.0",
    )
    go_repository(
        name = "org_golang_google_api",
        importpath = "google.golang.org/api",
        sum = "h1:Z9k22qD289SZ8gCJrk4DrWXkNjtfvKAUo/l1ma8eBYE=",
        version = "v0.150.0",
    )
    go_repository(
        name = "org_golang_google_appengine",
        importpath = "google.golang.org/appengine",
        sum = "h1:FZR1q0exgwxzPzp/aF+VccGrSfxfPpkBqjIIEq3ru6c=",
        version = "v1.6.7",
    )
    go_repository(
        name = "org_golang_google_genproto",
        importpath = "google.golang.org/genproto",
        sum = "h1:I6WNifs6pF9tNdSob2W24JtyxIYjzFB9qDlpUC76q+U=",
        version = "v0.0.0-20231030173426-d783a09b4405",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_api",
        importpath = "google.golang.org/genproto/googleapis/api",
        sum = "h1:HJMDndgxest5n2y77fnErkM62iUsptE/H8p0dC2Huo4=",
        version = "v0.0.0-20231030173426-d783a09b4405",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_bytestream",
        importpath = "google.golang.org/genproto/googleapis/bytestream",
        patches = ["//:patches/org_golang_google_genproto_googleapis_bytestream/service-registrar.diff"],
        sum = "h1:Da1Dicya3SJPSmfySsSj1cZAOOJBeD2TylBY/jXFTtw=",
        version = "v0.0.0-20231106174013-bbf56f31fb17",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_rpc",
        importpath = "google.golang.org/genproto/googleapis/rpc",
        sum = "h1:Jyp0Hsi0bmHXG6k9eATXoYtjd6e2UzZ1SCn/wIupY14=",
        version = "v0.0.0-20231106174013-bbf56f31fb17",
    )
    go_repository(
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable",
        importpath = "google.golang.org/grpc",
        sum = "h1:Z5Iec2pjwb+LEOqzpB2MR12/eKFhDPhuqW91O+4bwUk=",
        version = "v1.59.0",
    )
    go_repository(
        name = "org_golang_google_protobuf",
        build_extra_args = [
            "-exclude=**/testdata",
        ],
        importpath = "google.golang.org/protobuf",
        sum = "h1:g0LDEJHgrBl9N9r17Ru3sqWhkIx2NB67okBHPwC7hs8=",
        version = "v1.31.0",
    )
    go_repository(
        name = "org_golang_x_crypto",
        importpath = "golang.org/x/crypto",
        sum = "h1:frVn1TEaCEaZcn3Tmd7Y2b5KKPaZ+I32Q2OA3kYp5TA=",
        version = "v0.15.0",
    )
    go_repository(
        name = "org_golang_x_exp",
        importpath = "golang.org/x/exp",
        sum = "h1:c2HOrn5iMezYjSlGPncknSEr/8x5LELb/ilJbXi9DEA=",
        version = "v0.0.0-20190121172915-509febef88a4",
    )
    go_repository(
        name = "org_golang_x_lint",
        importpath = "golang.org/x/lint",
        patches = ["//:patches/org_golang_x_lint/generic.diff"],
        sum = "h1:2M3HP5CCK1Si9FQhwnzYhXdG6DXeebvUHFpre8QvbyI=",
        version = "v0.0.0-20201208152925-83fdc39ff7b5",
    )
    go_repository(
        name = "org_golang_x_mod",
        importpath = "golang.org/x/mod",
        sum = "h1:lFO9qtOdlre5W1jxS3r/4szv2/6iXxScdzjoBMXNhYk=",
        version = "v0.10.0",
    )
    go_repository(
        name = "org_golang_x_net",
        importpath = "golang.org/x/net",
        sum = "h1:mIYleuAkSbHh0tCv7RvjL3F6ZVbLjq4+R7zbOn3Kokg=",
        version = "v0.18.0",
    )
    go_repository(
        name = "org_golang_x_oauth2",
        importpath = "golang.org/x/oauth2",
        patches = ["//:patches/org_golang_x_oauth2/injectable-clock.diff"],
        sum = "h1:P0Vrf/2538nmC0H+pEQ3MNFRRnVR7RlqyVw+bvm26z0=",
        version = "v0.14.0",
    )
    go_repository(
        name = "org_golang_x_sync",
        importpath = "golang.org/x/sync",
        sum = "h1:60k92dhOjHxJkrqnwsfl8KuaHbn/5dl0lUPUklKo3qE=",
        version = "v0.5.0",
    )
    go_repository(
        name = "org_golang_x_sys",
        importpath = "golang.org/x/sys",
        sum = "h1:Vz7Qs629MkJkGyHxUlRHizWJRG2j8fbQKjELVSNhy7Q=",
        version = "v0.14.0",
    )
    go_repository(
        name = "org_golang_x_term",
        importpath = "golang.org/x/term",
        sum = "h1:LGK9IlZ8T9jvdy6cTdfKUCltatMFOehAQo9SRC46UQ8=",
        version = "v0.14.0",
    )
    go_repository(
        name = "org_golang_x_text",
        importpath = "golang.org/x/text",
        sum = "h1:ScX5w1eTa3QqT8oi6+ziP7dTV1S2+ALU0bI+0zXKWiQ=",
        version = "v0.14.0",
    )
    go_repository(
        name = "org_golang_x_time",
        importpath = "golang.org/x/time",
        sum = "h1:rg5rLMjNzMS1RkNLzCG38eapWhnYLFYXDXj2gOlr8j4=",
        version = "v0.3.0",
    )
    go_repository(
        name = "org_golang_x_tools",
        build_extra_args = [
            "-exclude=**/testdata",
            "-exclude=go/packages/packagestest",
        ],
        importpath = "golang.org/x/tools",
        sum = "h1:vSDcovVPld282ceKgDimkRSC8kpaH1dgyc9UMzlt84Y=",
        version = "v0.8.0",
    )
    go_repository(
        name = "org_golang_x_xerrors",
        importpath = "golang.org/x/xerrors",
        sum = "h1:H2TDz8ibqkAF6YGhCdN3jS9O0/s90v0rJh3X/OLHEUk=",
        version = "v0.0.0-20220907171357-04be3eba64a2",
    )
    go_repository(
        name = "org_uber_go_atomic",
        importpath = "go.uber.org/atomic",
        sum = "h1:ADUqmZGgLDDfbSL9ZmPxKTybcoEYHgpYfELNoN+7hsw=",
        version = "v1.7.0",
    )
    go_repository(
        name = "org_uber_go_goleak",
        importpath = "go.uber.org/goleak",
        sum = "h1:z+mqJhf6ss6BSfSM671tgKyZBFPTTJM+HLxnhPC3wu0=",
        version = "v1.1.10",
    )
    go_repository(
        name = "org_uber_go_multierr",
        importpath = "go.uber.org/multierr",
        sum = "h1:y6IPFStTAIT5Ytl7/XYmHvzXQ7S3g/IeZW9hyZ5thw4=",
        version = "v1.6.0",
    )
    go_repository(
        name = "org_uber_go_zap",
        importpath = "go.uber.org/zap",
        sum = "h1:CSUJ2mjFszzEWt4CdKISEuChVIXGBn3lAPwkRGyVrc4=",
        version = "v1.18.1",
    )
