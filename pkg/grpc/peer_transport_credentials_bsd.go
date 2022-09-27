//go:build darwin || freebsd
// +build darwin freebsd

package grpc

import (
	"golang.org/x/sys/unix"
)

func getPeerAuthInfoFromFileDescriptor(fd int) (PeerAuthInfo, error) {
	// Request the xucred structure from the kernel that corresponds
	// to this socket. It contains user ID and group membership data
	// of the peer.
	xucred, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return PeerAuthInfo{}, err
	}
	return PeerAuthInfo{
		UID:    xucred.Uid,
		Groups: xucred.Groups[:xucred.Ngroups],
	}, nil
}
