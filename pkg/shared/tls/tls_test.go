/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tls

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGenerate(t *testing.T) {
	t.Run("Create certificate with default options", func(t *testing.T) {
		certBytes, privKey, err := generate()
		assert.NoError(t, err)
		assert.NotNil(t, privKey)
		cert, err := x509.ParseCertificate(certBytes)
		assert.NoError(t, err)
		assert.NotNil(t, cert)
		assert.Len(t, cert.DNSNames, 1)
		assert.Equal(t, "localhost", cert.DNSNames[0])
		assert.Empty(t, cert.IPAddresses)
		assert.LessOrEqual(t, int64(time.Since(cert.NotBefore)), int64(10*time.Second))
	})
}

func TestGeneratePEM(t *testing.T) {
	t.Run("Create PEM from certficate options", func(t *testing.T) {
		cert, key, err := generatePEM()
		assert.NoError(t, err)
		assert.NotNil(t, cert)
		assert.NotNil(t, key)
	})

	t.Run("Create X509KeyPair", func(t *testing.T) {
		cert, err := GenerateX509KeyPair()
		assert.NoError(t, err)
		assert.NotNil(t, cert)
	})
}

func TestCreateCerts(t *testing.T) {
	t.Run("test create certs", func(t *testing.T) {
		sKey, serverCertPEM, caCertBytes, err := CreateCerts("test-org", []string{"test-host"}, time.Now().AddDate(1, 0, 0), true, false)
		assert.NoError(t, err)
		p, _ := pem.Decode(sKey)
		assert.Equal(t, "RSA PRIVATE KEY", p.Type)
		key, err := x509.ParsePKCS1PrivateKey(p.Bytes)
		assert.NoError(t, err)
		err = key.Validate()
		assert.NoError(t, err)
		sCert, err := validCertificate(serverCertPEM, t)
		assert.NoError(t, err)
		caParsedCert, err := validCertificate(caCertBytes, t)
		assert.NoError(t, err)
		assert.Equal(t, "test-host", caParsedCert.DNSNames[0])
		err = sCert.CheckSignatureFrom(caParsedCert)
		assert.NoError(t, err)
	})
}

func validCertificate(cert []byte, t *testing.T) (*x509.Certificate, error) {
	t.Helper()
	const certificate = "CERTIFICATE"
	caCert, _ := pem.Decode(cert)
	if caCert.Type != certificate {
		return nil, fmt.Errorf("CERT type mismatch, got %s, want: %s", caCert.Type, certificate)
	}
	parsedCert, err := x509.ParseCertificate(caCert.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cert, %w", err)
	}
	if parsedCert.SignatureAlgorithm != x509.SHA256WithRSA {
		return nil, fmt.Errorf("signature not match. Got: %s, want: %s", parsedCert.SignatureAlgorithm, x509.SHA256WithRSA)
	}
	return parsedCert, nil
}
