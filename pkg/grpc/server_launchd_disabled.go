//go:build !darwin

package grpc

import (
	"net"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newListenersFromLaunchdSocketName(name string) ([]net.Listener, error) {
	return nil, status.Error(codes.Unimplemented, "launchd socket activation is only supported on macOS")
}
