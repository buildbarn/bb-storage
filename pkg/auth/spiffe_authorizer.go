package auth

import (
	"context"
	"fmt"
	"log"
	"regexp"

	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/auth"
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
	if spifAuth != nil {
		return &SpiffeAuthorizer{spifAuth}
	}
	return &SpiffeAuthorizer{}
}

// Authorize implements the authorizer inferface
func (s *SpiffeAuthorizer) Authorize(ctx context.Context, instanceNames []digest.InstanceName) []error {
	fillErrors := func(err error) []error {
		errs := make([]error, len(instanceNames))
		if err != nil {
			for i := range errs {
				errs[i] = err
			}
		}
		return errs
	}
	// Extract client certificate chain from the connection.
	p, ok := peer.FromContext(ctx)
	if !ok {
		return fillErrors(status.Error(codes.Unauthenticated, "Connection was not established using gRPC"))
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return fillErrors(status.Error(codes.Unauthenticated, "Connection was not established using TLS"))
	}
	certs := tlsInfo.State.PeerCertificates
	if len(certs) == 0 {
		return fillErrors(status.Error(codes.Unauthenticated, "Client provided no TLS client certificate"))
	}
	id, err := x509svid.IDFromCert(certs[0])
	if err != nil {
		return fillErrors(err)
	}
	errs := fillErrors(nil)
	for i, instanceName := range instanceNames {
		if instanceName.String() == "" {
			// This is a bit dodgy.  SPIFFE authorization fails with a hierarchical action cache,
			// which includes the empty string as a valid instance name.  SPIFFE matchers don't
			// support empty instance names, and there is not really a way for clients to use them,
			// so just make them silently succeed here.  Because there is no way for clients to
			// create an action cache entry with an empty instance name, this is safe -- it isn't
			// possible to read them since they can't be created in the first place.
			continue
		}
		instanceMatcher, ok := s.InstanceNameSubjectMap[instanceName.String()]
		if !ok {
			errs[i] = status.Error(codes.PermissionDenied,
				fmt.Sprintf("instance name is not a match. available instance names are %s", instanceNames))
			continue
		}
		subjectMatchers, ok := instanceMatcher.AllowedSpiffeIds[id.TrustDomain().String()]
		if !ok {
			allowedTrustDomains := []string{}
			for k := range instanceMatcher.AllowedSpiffeIds {
				allowedTrustDomains = append(allowedTrustDomains, k)
			}
			errs[i] = status.Error(codes.PermissionDenied,
				fmt.Sprintf("trust domain not available. available trust domains are %s", allowedTrustDomains))
			continue
		}
		match, err := regexp.MatchString(subjectMatchers, id.Path())
		if err != nil {
			errs[i] = err
			continue
		}
		if !match {
			errs[i] = status.Error(codes.PermissionDenied, "spiffe id doesn't match pattern")
			log.Printf("SPIFFE Mismatch instance name %s, trust domain %s, path %s, matcher %s\n", instanceName.String(), id.TrustDomain().String(), id.Path(), subjectMatchers)
		}
	}
	return errs
}
