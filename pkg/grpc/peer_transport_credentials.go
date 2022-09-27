package grpc

import (
	"context"
	"net"
	"os"

	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// PeerAuthInfo contains peer credentials that were extracted by
// PeerTransportCredentials upon establishing a connection with the
// client over a UNIX socket.
//
// This structure is essentially an operating system independent version
// of struct ucred (Linux) and struct xucred (BSD).
type PeerAuthInfo struct {
	UID    uint32
	Groups []uint32
}

// AuthType returns a shorthand name for the type of credentials stored
// in this struct.
func (PeerAuthInfo) AuthType() string {
	return "peer"
}

type peerTransportCredentials struct{}

func (peerTransportCredentials) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, status.Error(codes.Unimplemented, "Cannot use peer transport credentials for client handshaking")
}

func (peerTransportCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	// Extract file descriptor of network connection.
	fileBackedConn, ok := conn.(interface{ File() (*os.File, error) })
	if !ok {
		return nil, nil, status.Error(codes.InvalidArgument, "Network connection is not backed by a file")
	}
	f, err := fileBackedConn.File()
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to obtain file for network connection")
	}
	defer f.Close()

	authInfo, err := getPeerAuthInfoFromFileDescriptor(int(f.Fd()))
	return conn, authInfo, err
}

func (peerTransportCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{}
}

func (peerTransportCredentials) Clone() credentials.TransportCredentials {
	return peerTransportCredentials{}
}

func (peerTransportCredentials) OverrideServerName(string) error {
	return nil
}

// PeerTransportCredentials is a gRPC TransportCredentials
// implementation that can be used by a gRPC server to extract
// credentials from a process connecting to the server over a UNIX
// socket.
var PeerTransportCredentials credentials.TransportCredentials = &peerTransportCredentials{}
