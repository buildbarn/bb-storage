{
  local platforms = [
    {
      name: 'linux_amd64',
      extension: '',
    },
    {
      name: 'linux_386',
      extension: '',
      testPlatform: 'linux_amd64',
    },
    {
      name: 'linux_arm',
      extension: '',
    },
    {
      name: 'linux_arm64',
      extension: '',
    },
    {
      name: 'darwin_amd64',
      extension: '',
    },
    {
      name: 'darwin_arm64',
      extension: '',
    },
    {
      name: 'freebsd_amd64',
      extension: '',
    },
    {
      name: 'windows_amd64',
      extension: '.exe',
    },
  ],

  local getJobs(binaries, containers, setupSteps, extraSteps, doUpload, enableCgo) = {
    build_and_test: {
      strategy: {
        matrix: {
          host: [
            {
              bazel_os: 'linux',
              cross_compile: true,
              lint: true,
              os: 'ubuntu-latest',
              platform_name: 'linux_amd64',
              upload: true,
            },
            {
              bazel_os: 'windows',
              cross_compile: false,
              lint: false,
              os: 'windows-latest',
              platform_name: 'windows_amd64',
              upload: false,
            },
          ],
        },
      },
      'runs-on': '${{ matrix.host.os }}',
      name: 'build_and_test ${{ matrix.host.os }}',
      steps: [
        // TODO: Switch back to l.gcr.io/google/bazel once updated
        // container images get published once again.
        // https://github.com/GoogleCloudPlatform/container-definitions/issues/12037
        {
          name: 'Check out source code',
          uses: 'actions/checkout@v1',
        },
      ] + setupSteps + [
        {
          name: 'Installing Bazel',
          run: 'v=$(cat .bazelversion) && curl -L https://github.com/bazelbuild/bazel/releases/download/${v}/bazel-${v}-${{matrix.host.bazel_os}}-x86_64 > ~/bazel && chmod +x ~/bazel && echo ~ >> ${GITHUB_PATH}',
          shell: 'bash',
        },
        {
          name: 'Override .bazelrc',
          // Use the D drive to improve performance.
          run: 'echo "startup --output_base=D:/bazel_output" >> .bazelrc',
          'if': "matrix.host.platform_name == 'windows_amd64'",
        },
        {
          name: 'Bazel mod tidy',
          run: 'bazel mod tidy',
          'if': 'matrix.host.lint',
        },
        {
          name: 'Gazelle',
          run: "rm -f $(find . -name '*.pb.go' | sed -e 's/[^/]*$/BUILD.bazel/') && bazel run //:gazelle",
          'if': 'matrix.host.lint',
        },
        {
          name: 'Buildifier',
          run: 'bazel run @com_github_bazelbuild_buildtools//:buildifier',
          'if': 'matrix.host.lint',
        },
        {
          name: 'Gofmt',
          run: 'bazel run @cc_mvdan_gofumpt//:gofumpt -- -w -extra $(pwd)',
          'if': 'matrix.host.lint',
        },
        {
          name: 'Clang format',
          run: "find . -name '*.proto' -exec bazel run @llvm_toolchain_llvm//:bin/clang-format -- -i {} +",
          'if': 'matrix.host.lint',
        },
        {
          name: 'GitHub workflows',
          run: 'bazel build //tools/github_workflows && cp bazel-bin/tools/github_workflows/*.yaml .github/workflows',
          'if': 'matrix.host.lint',
        },
        {
          name: 'Protobuf generation',
          run: |||
            if [ -d pkg/proto ]; then
              find . bazel-bin/pkg/proto -name '*.pb.go' -delete || true
              bazel build $(bazel query --output=label 'kind("go_proto_library", //...)')
              find bazel-bin/pkg/proto -name '*.pb.go' | while read f; do
                cat $f > $(echo $f | sed -e 's|.*/pkg/proto/|pkg/proto/|')
              done
            fi
          |||,
          'if': 'matrix.host.lint',
        },
        {
          name: 'Embedded asset generation',
          run: |||
            bazel build $(git grep '^[[:space:]]*//go:embed ' | sed -e 's|\(.*\)/.*//go:embed |//\1:|; s|"||g; s| .*||' | sort -u)
            git grep '^[[:space:]]*//go:embed ' | sed -e 's|\(.*\)/.*//go:embed |\1/|' | while read o; do
              if [ -e "bazel-bin/$o" ]; then
                rm -rf "$o"
                cp -r "bazel-bin/$o" "$o"
                find "$o" -type f -exec chmod -x {} +
              fi
            done
          |||,
          'if': 'matrix.host.lint',
        },
        {
          name: 'Test style conformance',
          run: 'git add . && git diff --exit-code HEAD --',
          'if': 'matrix.host.lint',
        },
        {
          name: 'Golint',
          run: 'bazel run @org_golang_x_lint//golint -- -set_exit_status $(pwd)/...',
          'if': 'matrix.host.lint',
        },
      ] + std.flattenArrays([
        [{
          name: platform.name + ": build${{ matrix.host.platform_name == '%s' && ' and test' || '' }}" % std.get(platform, 'testPlatform', platform.name),
          run: ('bazel %s --platforms=@rules_go//go/toolchain:%s ' % [
                  // Run tests only if we're not cross-compiling.
                  "${{ matrix.host.platform_name == '%s' && 'test --test_output=errors' || 'build' }}" % std.get(platform, 'testPlatform', platform.name),
                  platform.name + if enableCgo then '_cgo' else '',
                ]) + '//...',
          'if': "matrix.host.cross_compile || matrix.host.platform_name == '%s'" % platform.name,
        }] + (
          if doUpload
          then std.flattenArrays([
            [
              {
                name: '%s: copy %s' % [platform.name, binary],
                local executable = binary + platform.extension,
                run: 'rm -f %s && bazel run --run_under cp --platforms=@rules_go//go/toolchain:%s //cmd/%s $(pwd)/%s' % [
                  executable,
                  platform.name + if enableCgo then '_cgo' else '',
                  binary,
                  executable,
                ],
                'if': 'matrix.host.upload',
              },
              {
                name: '%s: upload %s' % [platform.name, binary],
                uses: 'actions/upload-artifact@v4',
                with: {
                  name: '%s.%s' % [binary, platform.name],
                  path: binary + platform.extension,
                },
                'if': 'matrix.host.upload',
              },
            ]
            for binary in binaries
          ])
          else []
        )
        for platform in platforms
      ]) + extraSteps + (
        if doUpload
        then (
          [
            {
              name: 'Install Docker credentials',
              run: 'echo "${GITHUB_TOKEN}" | docker login ghcr.io -u $ --password-stdin',
              env: {
                GITHUB_TOKEN: '${{ secrets.GITHUB_TOKEN }}',
              },
              'if': 'matrix.host.upload',
            },
          ] + [
            {
              name: 'Push container %s' % container,
              run: 'bazel run --stamp //cmd/%s_container_push' % container,
              'if': 'matrix.host.upload',
            }
            for container in containers
          ]
        )
        else []
      ),
    },
  },

  getWorkflows(binaries, containers, setupSteps=[], extraSteps=[]): {
    'master.yaml': {
      name: 'master',
      on: { push: { branches: ['main', 'master'] } },
      jobs: getJobs(binaries, containers, setupSteps, extraSteps, true, false),
    },
    'pull-requests.yaml': {
      name: 'pull-requests',
      on: { pull_request: { branches: ['main', 'master'] } },
      jobs: getJobs(binaries, containers, setupSteps, extraSteps, false, false),
    },
  },
}
