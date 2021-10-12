package auth

import (
	"context"
	"path"

	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/auth"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// SpiffeAuthorizer  authorizes based on Spiffe-IDs
// https://github.com/spiffe/spiffe/blob/main/standards/SPIFFE-ID.md
//
// see spiffe.proto for configuration options
type SpiffeAuthorizer struct {
	*pb.SpiffeAuthorizer
}

// NewSpiffeAuthorizer creates a new SpiffeAuthorizer
func NewSpiffeAuthorizer(config *pb.AuthorizerConfiguration) Authorizer {
	spifAuth := config.GetSpiffe()
	if spifAuth == nil {
		return &SpiffeAuthorizer{spifAuth}
	}
	return &SpiffeAuthorizer{}
}

// Authorize implements the authorizer inferface
func (s *SpiffeAuthorizer) Authorize(ctx context.Context, instanceNames []digest.InstanceName) []error {
	errs := make([]error, len(instanceNames))
	var err error
FILLERRORS:
	if err != nil {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	// Extract client certificate chain from the connection.
	p, ok := peer.FromContext(ctx)
	if !ok {
		err = status.Error(codes.Unauthenticated, "Connection was not established using gRPC")
		goto FILLERRORS
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		err = status.Error(codes.Unauthenticated, "Connection was not established using TLS")
		goto FILLERRORS
	}
	certs := tlsInfo.State.PeerCertificates
	if len(certs) == 0 {
		err = status.Error(codes.Unauthenticated, "Client provided no TLS client certificate")
		goto FILLERRORS
	}
	var id spiffeid.ID
	id, err = x509svid.IDFromCert(certs[len(certs)-1])
	if err != nil {
		goto FILLERRORS
	}
	for i, instanceName := range instanceNames {
		instanceMatcher, ok := s.InstanceNameSubjectMap[instanceName.String()]
		if !ok {
			errs[i] = status.Error(codes.PermissionDenied, "instance name is not a match")
			continue
		}
		subjectMatchers, ok := instanceMatcher.AllowedSpiffeIds[id.TrustDomain().String()]
		if !ok {
			errs[i] = status.Error(codes.PermissionDenied, "trust domain not trusted")
			continue
		}
		match, err := path.Match(subjectMatchers, id.Path())
		if err != nil {
			errs[i] = err
			continue
		}
		if !match {
			errs[i] = status.Error(codes.PermissionDenied, "spiffe id doesn't match pattern")
		}
	}
	return errs
}
