{
  local platforms = [
    {
      name: 'linux_amd64',
      buildAndTestCommand: 'test --test_output=errors',
      buildJustBinaries: false,
      extension: '',
    },
    {
      name: 'linux_386',
      buildAndTestCommand: 'test --test_output=errors',
      buildJustBinaries: false,
      extension: '',
    },
    {
      name: 'linux_arm',
      buildAndTestCommand: 'build',
      buildJustBinaries: false,
      extension: '',
    },
    {
      name: 'linux_arm64',
      buildAndTestCommand: 'build',
      buildJustBinaries: false,
      extension: '',
    },
    {
      name: 'darwin_amd64',
      buildAndTestCommand: 'build',
      buildJustBinaries: false,
      extension: '',
    },
    {
      name: 'darwin_arm64',
      buildAndTestCommand: 'build',
      buildJustBinaries: false,
      extension: '',
    },
    {
      name: 'freebsd_amd64',
      buildAndTestCommand: 'build',
      // Building '//...' is broken for FreeBSD, because rules_docker
      // doesn't want to initialize properly.
      buildJustBinaries: true,
      extension: '',
    },
    {
      name: 'windows_amd64',
      buildAndTestCommand: 'build',
      buildJustBinaries: false,
      extension: '.exe',
    },
  ],

  local getJobs(binaries, containers, doUpload) = {
    build_and_test: {
      'runs-on': 'ubuntu-latest',
      steps: [
        // TODO: Switch back to l.gcr.io/google/bazel once updated
        // container images get published once again.
        // https://github.com/GoogleCloudPlatform/container-definitions/issues/12037
        {
          name: 'Installing Bazel',
          run: 'curl -L https://github.com/bazelbuild/bazel/releases/download/5.0.0/bazel-5.0.0-linux-x86_64 > ~/bazel && chmod +x ~/bazel && echo ~ >> ${GITHUB_PATH}',
        },
        {
          name: 'Check out source code',
          uses: 'actions/checkout@v1',
        },
        {
          name: 'Restore Bazel cache',
          uses: 'actions/cache@v1',
          with: { key: 'bazel', path: '~/.cache/bazel' },
        },
        {
          name: 'Gazelle',
          run: 'bazel run //:gazelle -- update-repos -from_file=go.mod -to_macro go_dependencies.bzl%go_dependencies -prune && bazel run //:gazelle',
        },
        {
          name: 'Buildifier',
          run: "sed '/^$/d' go_dependencies.bzl > go_dependencies.bzl.new && mv go_dependencies.bzl.new go_dependencies.bzl && bazel run @com_github_bazelbuild_buildtools//:buildifier",
        },
        {
          name: 'Gofmt',
          run: 'bazel run @cc_mvdan_gofumpt//:gofumpt -- -lang 1.18 -w -extra $(pwd)',
        },
        {
          name: 'Clang format',
          run: "find . -name '*.proto' -exec bazel run @llvm_toolchain_llvm//:bin/clang-format -- -i {} +",
        },
        {
          name: 'GitHub workflows',
          run: 'bazel build //tools/github_workflows && cp bazel-bin/tools/github_workflows/*.yaml .github/workflows',
        },
        {
          name: 'Protobuf generation',
          run: |||
            bazel build $(bazel query 'kind("go_proto_library", //...)')
            find . -name '*.pb.go' -delete
            find bazel-bin/pkg/proto -name '*.pb.go' | while read f; do
              cat $f > $(echo $f | sed -e 's|.*/pkg/proto/|pkg/proto/|')
            done
          |||,
        },
        {
          name: 'Test style conformance',
          run: 'git diff --exit-code HEAD --',
        },
        {
          name: 'Golint',
          run: 'bazel run @org_golang_x_lint//golint -- -set_exit_status $(pwd)/...',
        },
        {
          name: 'Check for ineffective assignments',
          run: 'bazel run @com_github_gordonklaus_ineffassign//:ineffassign $(pwd)',
        },
      ] + std.flattenArrays([
        [{
          name: platform.name + ': build and test',
          run: ('bazel %s --platforms=@io_bazel_rules_go//go/toolchain:%s ' % [
                  platform.buildAndTestCommand,
                  platform.name,
                ]) + (
            if platform.buildJustBinaries
            then std.join(' ', ['//cmd/' + binary for binary in binaries])
            else '//...'
          ),
        }] + (
          if doUpload
          then std.flattenArrays([
            [
              {
                name: '%s: copy %s' % [platform.name, binary],
                local executable = binary + platform.extension,
                run: 'rm -f %s && bazel run --run_under cp --platforms=@io_bazel_rules_go//go/toolchain:%s //cmd/%s $(pwd)/%s' % [executable, platform.name, binary, executable],
              },
              {
                name: '%s: upload %s' % [platform.name, binary],
                uses: 'actions/upload-artifact@v2-preview',
                with: {
                  name: '%s.%s' % [binary, platform.name],
                  path: binary + platform.extension,
                },
              },
            ]
            for binary in binaries
          ])
          else []
        )
        for platform in platforms
      ]) + (
        if doUpload
        then (
          [
            {
              name: 'Install Docker credentials',
              run: 'mkdir -p ~/.docker && echo "${DOCKER_CONFIG_JSON}" > ~/.docker/config.json',
              env: {
                DOCKER_CONFIG_JSON: '${{ secrets.DOCKER_CONFIG_JSON }}',
              },
            },
          ] + [
            {
              name: 'Push container %s' % container,
              run: 'bazel run //cmd/%s_container_push' % container,
            }
            for container in containers
          ]
        )
        else []
      ),
    },
  },

  getWorkflows(binaries, containers): {
    'master.yaml': {
      name: 'master',
      on: { push: { branches: ['master'] } },
      jobs: getJobs(binaries, containers, true),
    },
    'pull-requests.yaml': {
      name: 'pull-requests',
      on: { pull_request: { branches: ['master'] } },
      jobs: getJobs(binaries, containers, false),
    },
  },
}
