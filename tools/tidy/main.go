package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
)

func doRun(abspath string, arg ...string) {
	cmd := exec.Command(abspath, arg...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("%s failed: %v", abspath, err)
	}
}

func run(path string, arg ...string) {
	abspath, err := bazel.Runfile(path)
	if err != nil {
		log.Fatalf("%s not found in runfiles: %v", path, err)
	}
	doRun(abspath, arg...)
}

func runGo(pkg, name string, arg ...string) {
	abspath, found := bazel.FindBinary(pkg, name)
	if !found {
		log.Fatalf("%s/%s not found in runfiles", pkg, name)
	}
	doRun(abspath, arg...)
}

func copyWorkflows(path, root string) {
	abspath, err := bazel.Runfile(path)
	if err != nil {
		log.Fatal("could not find %s: %v", path, err)
	}
	contents, err := ioutil.ReadDir(abspath)
	if err != nil {
		log.Fatal("could not read %s: %v", abspath, err)
	}
	for _, info := range contents {
		if !info.IsDir() {
			inpath := filepath.Join(abspath, info.Name())
			outpath := filepath.Join(root, ".github/workflows", info.Name())

			data, err := ioutil.ReadFile(inpath)
			if err != nil {
				log.Fatal("could not read %s: %v", inpath, err)
			}
			err = ioutil.WriteFile(outpath, data, 0644)
			if err != nil {
				log.Fatal("could not write %s: %v", outpath, err)
			}
		}
	}
}

func main() {
	root := os.Getenv("BUILD_WORKSPACE_DIRECTORY")
	if root == "" {
		log.Fatalf("invoke as bazel run //tools/tidy")
	}
	run("buildifier.bash")
	run("gazelle-runner.bash", "-bazel_run", "update-repos", "-from_file=go.mod", "-to_macro", "go_dependencies.bzl%go_dependencies", "-prune")
	run("gazelle-runner.bash", "-bazel_run")
	run("../go_sdk/bin/gofmt", "-s", "-w", root)
	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && info.Name() == ".git" {
			return filepath.SkipDir
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".proto") {
			run("external/llvm_toolchain/bin/clang-format", "-i", path)
		}
		return nil
	}); err != nil {
		log.Fatalf("walking %s failed: %v", root, err)
	}
	runGo("../org_golang_x_lint/golint", "golint", "-set_exit_status", root+"/...")
	runGo("../com_github_gordonklaus_ineffassign", "ineffassign", root)
	copyWorkflows("tools/github_workflows", root)
}
