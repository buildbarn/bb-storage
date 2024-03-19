//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package grpcauth_test

import (
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/grpcauth"
	"github.com/stretchr/testify/require"
)

func TestPeerTransportCredentials(t *testing.T) {
	// Create network connections through socketpair().
	fds, err := syscall.Socketpair(syscall.AF_LOCAL, syscall.SOCK_STREAM, 0)
	require.NoError(t, err)

	file0 := os.NewFile(uintptr(fds[0]), "socket0")
	conn0, err := net.FileConn(file0)
	require.NoError(t, err)
	require.NoError(t, file0.Close())

	file1 := os.NewFile(uintptr(fds[1]), "socket1")
	conn1, err := net.FileConn(file1)
	require.NoError(t, err)
	require.NoError(t, file1.Close())

	// Perform server handshake.
	wrappedConn0, authInfo, err := grpcauth.PeerTransportCredentials.ServerHandshake(conn0)
	require.NoError(t, err)

	// Resulting credentials should match that of the current user.
	peerAuthInfo := authInfo.(grpcauth.PeerAuthInfo)
	require.Equal(t, uint32(syscall.Getuid()), peerAuthInfo.UID)
	require.LessOrEqual(t, 1, len(peerAuthInfo.Groups))
	require.Equal(t, uint32(syscall.Getgid()), peerAuthInfo.Groups[0])

	require.NoError(t, wrappedConn0.Close())
	require.NoError(t, conn1.Close())
}
