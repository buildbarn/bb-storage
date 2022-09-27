//go:build windows
// +build windows

package grpc

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func getPeerAuthInfoFromFileDescriptor(fd int) (PeerAuthInfo, error) {
	return PeerAuthInfo{}, status.Error(codes.Unimplemented, "Peer authentication information cannot be extracted on this platform")
}
