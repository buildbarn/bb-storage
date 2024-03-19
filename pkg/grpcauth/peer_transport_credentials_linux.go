//go:build linux
// +build linux

package grpcauth

import (
	"golang.org/x/sys/unix"
)

func getPeerAuthInfoFromFileDescriptor(fd int) (PeerAuthInfo, error) {
	// Request the ucred structure from the kernel that corresponds
	// to this socket. It contains user ID and group membership data
	// of the peer.
	ucred, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return PeerAuthInfo{}, err
	}
	return PeerAuthInfo{
		UID:    ucred.Uid,
		Groups: []uint32{ucred.Gid},
	}, nil
}
