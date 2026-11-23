//go:build darwin

package grpc

import (
	"net"

	"github.com/bored-engineer/go-launchd"
)

func newListenersFromLaunchdSocketName(name string) ([]net.Listener, error) {
	return launchd.Sockets(name)
}
