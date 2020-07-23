// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package authority provides an in-process TLS certificate authority,
// useful for creating and distributing TLS certificates for mutually authenticated
// HTTPS networking within Bigmachine.
package authority

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"time"
)

// DriftMargin is the amount of acceptable clock drift during
// certificate issuing and verification.
const DriftMargin = time.Minute

// CertDuration is the duration of cert validity for the certificates
// issued by authorities.
const certDuration = 7 * 24 * time.Hour

//  A T is a TLS certificate authority which can issue client and server
// certificates and provide configuration for HTTPS clients.
type T struct {
	key  *rsa.PrivateKey
	cert *x509.Certificate

	// The CA certificate and key are stored in PEM-encoded bytes
	// as most of the Go APIs operate directly on these.
	certPEM, keyPEM []byte
}

// New creates a new certificate authority, reading the PEM-encoded
// certificate and private key from the provided path. If the path
// does not exist, newCA instead creates a new certificate authority
// and stores it at the provided path. If path is empty, the
// authority is ephemeral.
func New(filename string) (*T, error) {
	// As an extra precaution, we always exercise the read path, so if
	// the CA PEM is missing, we generate it, and then read it back.
	pemBlock, err := cached(filename, func() ([]byte, error) {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, err
		}
		template := x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject:      pkix.Name{CommonName: "bigmachine"},
			NotBefore:    time.Now().Add(-DriftMargin),
			// Newton says we have at least this long:
			//	https://newtonprojectca.files.wordpress.com/2013/06/reply-to-tom-harpur-2-page-full-version.pdf
			NotAfter: time.Date(2060, 1, 1, 0, 0, 0, 0, time.UTC),

			KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			BasicConstraintsValid: true,
			IsCA:                  true,
		}
		cert, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
		if err != nil {
			return nil, err
		}
		var b bytes.Buffer
		// Save it also.
		if err := pem.Encode(&b, &pem.Block{Type: "CERTIFICATE", Bytes: cert}); err != nil {
			return nil, err
		}
		if err := pem.Encode(&b, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}); err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not build CA: %v", err)
	}

	var certBlock, keyBlock []byte
	for {
		var derBlock *pem.Block
		derBlock, pemBlock = pem.Decode(pemBlock)
		if derBlock == nil {
			break
		}
		switch derBlock.Type {
		case "CERTIFICATE":
			certBlock = derBlock.Bytes
		case "RSA PRIVATE KEY":
			keyBlock = derBlock.Bytes
		}
	}

	if certBlock == nil || keyBlock == nil {
		return nil, errors.New("httpsca: incomplete certificate")
	}
	ca := new(T)
	ca.cert, err = x509.ParseCertificate(certBlock)
	if err != nil {
		return nil, err
	}
	ca.key, err = x509.ParsePKCS1PrivateKey(keyBlock)
	if err != nil {
		return nil, err
	}
	ca.certPEM, err = encodePEM(&pem.Block{Type: "CERTIFICATE", Bytes: certBlock})
	if err != nil {
		return nil, err
	}
	ca.keyPEM, err = encodePEM(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(ca.key)})
	if err != nil {
		return nil, err
	}
	return ca, nil
}

// Cert returns the authority's x509 certificate.
func (c *T) Cert() *x509.Certificate {
	return c.cert
}

// Issue issues a new certificate out of this CA with the provided common name, ttl, ips, and DNSes.
func (c *T) Issue(cn string, ttl time.Duration, ips []net.IP, dnss []string) ([]byte, *rsa.PrivateKey, error) {
	maxSerial := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, maxSerial)
	if err != nil {
		return nil, nil, err
	}
	now := time.Now().Add(-DriftMargin)
	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: cn,
		},
		NotBefore:             now,
		NotAfter:              now.Add(DriftMargin + ttl),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	template.IPAddresses = append(template.IPAddresses, ips...)
	template.DNSNames = append(template.DNSNames, dnss...)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.CreateCertificate(rand.Reader, &template, c.cert, &key.PublicKey, c.key)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

// HTTPSConfig returns a tls configs based on newly issued TLS certificates from this CA.
func (c *T) HTTPSConfig() (client, server *tls.Config, err error) {
	cert, key, err := c.Issue("bigmachine", certDuration, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(c.certPEM)

	// Load the newly created certificate.
	certPEM, err := encodePEM(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err := encodePEM(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err != nil {
		return nil, nil, err
	}
	tlscert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, nil, err
	}
	clientConfig := &tls.Config{
		RootCAs:            pool,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{tlscert},
	}
	serverConfig := &tls.Config{
		ClientCAs:    pool,
		Certificates: []tls.Certificate{tlscert},
	}
	return clientConfig, serverConfig, nil
}

func encodePEM(block *pem.Block) ([]byte, error) {
	var w bytes.Buffer
	if err := pem.Encode(&w, block); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func cached(filename string, gen func() ([]byte, error)) ([]byte, error) {
	if filename == "" {
		return gen()
	}
	p, err := ioutil.ReadFile(filename)
	if err == nil || !os.IsNotExist(err) {
		return p, nil
	}
	p, err = gen()
	if err != nil {
		return nil, err
	}
	return p, ioutil.WriteFile(filename, p, 0600)
}
