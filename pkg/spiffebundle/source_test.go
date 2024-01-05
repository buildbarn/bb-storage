package spiffebundle

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

func TestGCPCertSucceeds(t *testing.T) {
	filename := testutil.MakeCaPemFile(t, "spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Errorf("can't read creds: %v", err)
	}
	td, err := spiffeid.TrustDomainFromString("spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
	if err != nil {
		t.Errorf("can't convert string to trust domain: %v", err)
	}
	src := New()
	bundle, err := x509bundle.Parse(td, b)
	if err != nil {
		t.Errorf("can't parse bundle: %v", err)
	}
	src.Add(bundle, ".svc.id.goog")

	foundBundle, err := src.GetX509BundleForTrustDomain(td)
	if err != nil {
		t.Errorf("GetX509BundleForTrustDomain failed: %v", err)
	}
	if !bundle.Equal(foundBundle) {
		t.Error("Found different bundle")
	}
}

func TestGCPCertFails(t *testing.T) {
	filename := testutil.MakeCaPemFile(t, "spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Errorf("can't read creds: %v", err)
	}
	td, err := spiffeid.TrustDomainFromString("spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
	if err != nil {
		t.Errorf("can't convert string to trust domain: %v", err)
	}
	src := New()
	bundle, err := x509bundle.Parse(td, b)
	if err != nil {
		t.Errorf("can't parse bundle: %v", err)
	}
	src.Add(bundle, ".onprem.signed.goog")

	foundBundle, err := src.GetX509BundleForTrustDomain(td)
	if err == nil {
		t.Errorf("GetX509BundleForTrustDomain should have failed")
	}
	if foundBundle != nil {
		t.Error("Found a bundle but shouldn't have")
	}
}

func TestMultiGCPCertSucceeds(t *testing.T) {
	filename := testutil.MakeCaPemFile(t, "spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Errorf("can't read creds: %v", err)
	}
	td1, err := spiffeid.TrustDomainFromString("spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
	if err != nil {
		t.Errorf("can't convert string to trust domain: %v", err)
	}
	td2, err := spiffeid.TrustDomainFromString("spiffe://acme.com.onprem.signed.goog/ns/project-id/sa/system-acct")

	if err != nil {
		t.Errorf("can't convert string to trust domain: %v", err)
	}
	src := New()
	bundle, err := x509bundle.Parse(td1, b)
	if err != nil {
		t.Errorf("can't parse bundle: %v", err)
	}
	src.Add(bundle, ".svc.id.goog", ".onprem.signed.goog")

	foundBundle, err := src.GetX509BundleForTrustDomain(td1)
	if err != nil {
		t.Errorf("GetX509BundleForTrustDomain failed: %v", err)
	}
	if !bundle.Equal(foundBundle) {
		t.Error("Found different bundle")
	}
	foundBundle, err = src.GetX509BundleForTrustDomain(td2)
	if err != nil {
		t.Errorf("GetX509BundleForTrustDomain failed: %v", err)
	}
	if !bundle.Equal(foundBundle) {
		t.Error("Found different bundle")
	}
}

func TestSubstringMatchesAtMostOneTrustDomain(t *testing.T) {
	var tds []string
	patterns := [...]string{".svc.id.goog", ".onprem.signed.goog"}
	td, err := spiffeid.TrustDomainFromString("spiffe://acme.com.svc.id.goog/ns/project-id/sa/system-acct")
	if err != nil {
		t.Errorf("can't convert string to trust domain: %v", err)
	}
	tds = append(tds, td.String())
	td, err = spiffeid.TrustDomainFromString("spiffe://acme.com.onprem.signed.goog/ns/project-id/sa/system-acct")
	if err != nil {
		t.Errorf("can't convert string to trust domain: %v", err)
	}
	tds = append(tds, td.String())
	for _, p := range patterns {
		match := 0
		for _, ts := range tds {
			if strings.Contains(ts, p) {
				match++
			}
		}
		if match > 1 {
			t.Error("pattern matched more than one trust domain")
		}
		if match == 0 {
			t.Error("pattern didn't match any trust domain")
		}
	}
}
