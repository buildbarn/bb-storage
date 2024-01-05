package spiffebundle

import (
	"fmt"
	"strings"
	"sync"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// With GKE in GCP and workload identity certificates enabled, the server and clients share a common trust bundle
// when they are all hosted in GKE.  The go-spiffe library doesn't support this well, as it tries to find a trust
// bundle associated with a trust domain, and if none is found, verification fails.  If the server and its clients
// share a trust bundle, there is no reason to use SPIFFE federation.  It is much easier to write a custom bundle
// source, so here we are.
//
// This framework allows you to inject a custom bundle source into the SPIFFE workflow.  How to exactly get the
// bundles from disparate portions of your organization is up to you.  Similarly, you can customize this for
// whatever matching rules make sense for your organization.  Here we allow multiple trust domains to share the
// same trust bundle.
//
// Note: Reloading certs is done outside the scope of the GCPSource.  The new certs can be added and they'll replace the
// old certs as long as the set of matching keys remain the same.

type GCPSource struct {
	x509bundle.Source // implements GetX509BundleForTrustDomain()
	mu                sync.RWMutex

	bundles map[string]*x509bundle.Bundle
}

func New() *GCPSource {
	b := make(map[string]*x509bundle.Bundle)
	return &GCPSource{
		bundles: b,
	}
}

func (gs *GCPSource) Add(bundle *x509bundle.Bundle, keys ...string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	for _, k := range keys {
		gs.bundles[k] = bundle
	}
}

// Match on substring of trust domain to determine which bundle to use.  For GCP, the trust domain ends with ".svc.id.goog"
func (gs *GCPSource) GetX509BundleForTrustDomain(trustDomain spiffeid.TrustDomain) (*x509bundle.Bundle, error) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	for k, b := range gs.bundles {
		if strings.Contains(trustDomain.String(), k) {
			return b, nil
		}
	}
	return nil, fmt.Errorf("no bundle found for trust domain %s\n", trustDomain.String())
}
