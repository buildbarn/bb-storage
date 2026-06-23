package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	"github.com/buildbarn/bb-storage/pkg/proto/configuration/partition_ephemeral_disks"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// partition_ephemeral_disks: Place all local SSDs of a cloud compute
// instance in a single LVM2 volume group, and partition it.
//
// This command can be run as part of a daemonset on all nodes in a
// given Kubernetes node group to ensure that the ephemeral storage
// present on these systems is partitioned properly. That way other pods
// can make use of raw block devices with fine-grained sizes.

func run(command string, arguments ...string) error {
	cmd := exec.Command(command, arguments...)
	// lvcreate may hang if the following environment variable is
	// not set, as it can't post device creation notifications to
	// udev on the host system. By setting this environment
	// variable, lvcreate will create new device nodes under /dev
	// directly.
	cmd.Env = append(cmd.Environ(), "DM_DISABLE_UDEV=y")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: partition_ephemeral_disks partition_ephemeral_disks.jsonnet")
	}
	var configuration partition_ephemeral_disks.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}

	// Determine which devices need to go in the volume group.
	unexpandedDevicePaths, err := filepath.Glob(configuration.EphemeralDiskDevicesPattern)
	if err != nil {
		log.Fatal("Failed to obtain paths of ephemeral disk devices: ", err)
	}
	expandedDevicePaths := make(map[string]struct{}, len(unexpandedDevicePaths))
	for _, unexpandedDevicePath := range unexpandedDevicePaths {
		expandedDevicePath, err := filepath.EvalSymlinks(unexpandedDevicePath)
		if err != nil {
			log.Fatalf("Cannot evaluate symbolic links for path %#v: %s", unexpandedDevicePath, err)
		}
		expandedDevicePaths[expandedDevicePath] = struct{}{}
	}
	if len(expandedDevicePaths) == 0 {
		log.Fatalf("Pattern %#v does not expand to any ephemeral disk devices", configuration.EphemeralDiskDevicesPattern)
	}

	// Create the volume group if needed.
	volumeGroupName := "ephemeral"
	if err := run("vgs", volumeGroupName); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 5 {
			log.Fatalf("Failed to determine existence of volume group %#v: %s", volumeGroupName, err)
		}

		arguments := []string{"--force", volumeGroupName}
		for expandedDevicePath := range expandedDevicePaths {
			arguments = append(arguments, expandedDevicePath)
		}
		if err := run("vgcreate", arguments...); err != nil {
			log.Fatalf("Failed to create volume group %#v: %s", volumeGroupName, err)
		}
	}

	// Check validity of the provided partitions.
	totalPercentage := int32(0)
	for _, partition := range configuration.Partitions {
		if partition.SizePercentage < 1 || partition.SizePercentage > 100 {
			log.Fatalf("Partition %#v has illegal size %d", partition.Name, partition.SizePercentage)
		}
		totalPercentage += partition.SizePercentage
	}
	if totalPercentage != 100 {
		log.Fatal("Partition sizes do not add up to 100%")
	}

	// Create the partitions inside the ephemeral volume group.
	var baseArguments []string
	if len(expandedDevicePaths) > 1 {
		baseArguments = []string{
			"--type", "raid0",
			"--stripes", strconv.FormatInt(int64(len(expandedDevicePaths)), 10),
			"--stripesize", "2048",
		}
	}
	for _, partition := range configuration.Partitions {
		if err := run("lvcreate", append(
			append([]string(nil), baseArguments...),
			"--extents", fmt.Sprintf("%d%%VG", partition.SizePercentage),
			"--name", partition.Name,
			"--wipesignatures", "n",
			volumeGroupName,
		)...); err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 5 {
				log.Fatalf("Failed to create logical volume %#v: %s", partition.Name, err)
			}
		}
	}
}
