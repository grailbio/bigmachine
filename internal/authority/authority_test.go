// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package authority_test

import (
	"crypto/rsa"
	"crypto/x509"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/grailbio/bigmachine/internal/authority"
)

func TestCA(t *testing.T) {
	name, cleanup := tempFile(t)
	defer cleanup()
	ca, err := authority.New(name)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	ips := []net.IP{net.IPv4(1, 2, 3, 4)}
	dnses := []string{"test.grail.com"}
	certBytes, priv, err := ca.Issue("test", 10*time.Minute, ips, dnses)
	if err != nil {
		t.Fatal(err)
	}
	verify(t, ca, now, certBytes, priv, ips, dnses)

	// Make sure that when we restore the CA, it's still a valid cert.
	ca, err = authority.New(name)
	if err != nil {
		t.Fatal(err)
	}
	verify(t, ca, now, certBytes, priv, ips, dnses)
}

func TestCAEphemeral(t *testing.T) {
	ca, err := authority.New("")
	now := time.Now()
	ips := []net.IP{net.IPv4(1, 2, 3, 4)}
	dnses := []string{"test2.grail.com"}
	certBytes, priv, err := ca.Issue("test", 10*time.Minute, ips, dnses)
	if err != nil {
		t.Fatal(err)
	}
	verify(t, ca, now, certBytes, priv, ips, dnses)
}

func verify(t *testing.T, ca *authority.T, now time.Time, certBytes []byte, priv *rsa.PrivateKey, ips []net.IP, dnses []string) {
	t.Helper()
	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		t.Fatal(err)
	}
	opts := x509.VerifyOptions{}
	opts.Roots = x509.NewCertPool()
	opts.Roots.AddCert(ca.Cert())
	if _, err := cert.Verify(opts); err != nil {
		t.Fatal(err)
	}
	if err := priv.Validate(); err != nil {
		t.Fatal(err)
	}
	if got, want := priv.Public(), cert.PublicKey; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cert.Subject.CommonName, "test"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := cert.NotBefore, now.Add(-authority.DriftMargin); want.Before(got) {
		t.Errorf("wanted %s <= %s", got, want)
	}
	if got, want := cert.NotAfter.Sub(cert.NotBefore), 10*time.Minute+authority.DriftMargin; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if cert.IsCA {
		t.Error("cert is CA")
	}
	if got, want := cert.IPAddresses, ips; !ipsEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cert.DNSNames, dnses; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func tempFile(t *testing.T) (string, func()) {
	// Test that the CA generates valid certs.
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	return name, func() { os.Remove(name) }
}

func ipsEqual(x, y []net.IP) bool {
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if !x[i].Equal(y[i]) {
			return false
		}
	}
	return true
}
