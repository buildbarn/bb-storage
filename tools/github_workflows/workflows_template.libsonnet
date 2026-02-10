{
  local platforms = [
    {
      name: 'linux_amd64',
      extension: '',
    },
    {
      name: 'linux_amd64_v3',
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
              os: 'ubuntu-latest',
              platform_name: 'linux_amd64',
              upload: true,
            },
            {
              bazel_os: 'windows',
              cross_compile: false,
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
      ] + std.flattenArrays([
        [{
          name: platform.name + ": build${{ matrix.host.platform_name == '%s' && ' and test' || '' }}" % std.get(platform, 'testPlatform', platform.name),
          // Run tests only if we're not cross-compiling.
          run: "bazel ${{ matrix.host.platform_name == '%s' && 'test --test_output=errors' || 'build --platforms=@com_github_buildbarn_bb_storage//tools/platforms:%s --@bazel_tools//tools/test:incompatible_use_default_test_toolchain=False' }} //..." % [
            std.get(platform, 'testPlatform', platform.name),
            platform.name + if enableCgo then '_cgo' else '',
          ],
          'if': "matrix.host.cross_compile || matrix.host.platform_name == '%s'" % platform.name,
        }] + (
          if doUpload
          then std.flattenArrays([
            [
              {
                name: '%s: copy %s' % [platform.name, binary],
                local executable = binary + platform.extension,
                run: 'rm -f %s && bazel run --run_under cp --platforms=@com_github_buildbarn_bb_storage//tools/platforms:%s //cmd/%s $(pwd)/%s' % [
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
            {
              name: 'Clean Bazel cache from previous steps',
              run: 'bazel clean',
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
    [if doUpload then 'publish']: {
      'runs-on': 'ubuntu-latest',
      name: 'publish',
      needs: ['build_and_test'],
      steps: [
        {
          name: 'Checkout the repository',
          uses: 'actions/checkout@v4 ',
        },
        {
          name: 'Download release artifacts',
          uses: 'actions/download-artifact@v4',
          with: {
            'merge-multiple': true,
            path: 'assets',
          },
        },
        {
          name: 'Calculate shasum',
          run: 'shasum -a 256 assets/* > sha256',
        },
        {
          name: 'Set release Git user',
          run: |||
            git config --global user.email "41898282+github-actions[bot]@users.noreply.github.com"
            git config --global user.name "github-actions[bot]"
          |||,
        },
        {
          name: 'Generate tag',
          id: 'create_tag',
          run: |||
            BUILD_SCM_REVISION=$(git rev-parse --short HEAD)
            BUILD_SCM_TIMESTAMP=$(TZ=UTC date --date "@$(git show -s --format=%ct HEAD)" +%Y%m%dT%H%M%SZ)
            RELEASE_TAG="${BUILD_SCM_TIMESTAMP}-${BUILD_SCM_REVISION}}"
            git tag $RELEASE_TAG
            git push --follow-tags
            echo "release_tag=$RELEASE_TAG" >> $GITHUB_OUTPUT
          |||,
        },
        {
          name: 'Create GitHub release and upload artifacts',
          uses: 'softprops/action-gh-release@v2',
          with: {
            make_latest: true,
            tag_name: '${{ steps.create_tag.outputs.release_tag }}',
            files: |||
              assets/*
              sha256
            |||,
          },
        },
      ],
    },
    lint: {
      'runs-on': 'ubuntu-latest',
      name: 'lint',
      steps: [
        {
          name: 'Check out source code',
          uses: 'actions/checkout@v1',
        },
      ] + setupSteps + [
        {
          name: 'Installing Bazel',
          run: 'v=$(cat .bazelversion) && curl -L https://github.com/bazelbuild/bazel/releases/download/${v}/bazel-${v}-linux-x86_64 > ~/bazel && chmod +x ~/bazel && echo ~ >> ${GITHUB_PATH}',
          shell: 'bash',
        },
        {
          name: 'Reformat',
          run: 'bazel run @com_github_buildbarn_bb_storage//tools:reformat',
        },
        {
          name: 'Test style conformance',
          run: 'git add . && git diff --exit-code HEAD --',
        },
        {
          name: 'Golint',
          run: 'bazel run @org_golang_x_lint//golint -- -set_exit_status $(pwd)/...',
        },
      ],
    },
  },

  getWorkflows(binaries, containers, setupSteps=[], extraSteps=[]): {
    'main.yaml': {
      name: 'main',
      on: { push: { branches: ['main'] } },
      jobs: getJobs(binaries, containers, setupSteps, extraSteps, true, false),
    },
    'pull-requests.yaml': {
      name: 'pull-requests',
      on: { pull_request: { branches: ['main'] } },
      jobs: getJobs(binaries, containers, setupSteps, extraSteps, false, false),
    },
  },
}
