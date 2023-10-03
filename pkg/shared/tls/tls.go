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
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"time"
)

func pemBlockForKey(priv interface{}) *pem.Block {
	switch k := priv.(type) {
	case *ecdsa.PrivateKey:
		b, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			log.Fatal(err)
		}
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
	default:
		return nil
	}
}

func generate() ([]byte, crypto.PrivateKey, error) {
	hosts := []string{"localhost"}

	var err error
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate private key: %s", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate serial number: %s", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Numaproj"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create certificate: %s", err)
	}
	return certBytes, privateKey, nil
}

// generatePEM generates a new certificate and key and returns it as PEM encoded bytes
func generatePEM() ([]byte, []byte, error) {
	certBytes, privateKey, err := generate()
	if err != nil {
		return nil, nil, err
	}
	certpem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	keypem := pem.EncodeToMemory(pemBlockForKey(privateKey))
	return certpem, keypem, nil
}

// GenerateX509KeyPair generates a X509 key pair
func GenerateX509KeyPair() (*tls.Certificate, error) {
	certpem, keypem, err := generatePEM()
	if err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(certpem, keypem)
	if err != nil {
		return nil, err
	}
	return &cert, nil
}

func certTemplate(org string, hosts []string, notAfter time.Time) (*x509.Certificate, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to generate serial number, %w", err)
	}
	return &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{org},
		},
		SignatureAlgorithm:    x509.SHA256WithRSA,
		NotBefore:             time.Now(),
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
		DNSNames:              hosts,
	}, nil
}

func createCACertTemplate(org string, hosts []string, notAfter time.Time) (*x509.Certificate, error) {
	rootCert, err := certTemplate(org, hosts, notAfter)
	if err != nil {
		return nil, err
	}
	rootCert.IsCA = true
	rootCert.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	rootCert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	return rootCert, nil
}

// Sign the cert
func createCert(template, parent *x509.Certificate, pub, parentPriv interface{}) (
	cert *x509.Certificate, certPEM []byte, err error) {
	certDER, err := x509.CreateCertificate(rand.Reader, template, parent, pub, parentPriv)
	if err != nil {
		return
	}
	cert, err = x509.ParseCertificate(certDER)
	if err != nil {
		return
	}
	b := pem.Block{Type: "CERTIFICATE", Bytes: certDER}
	certPEM = pem.EncodeToMemory(&b)
	return
}

func createCA(org string, hosts []string, notAfter time.Time) (*rsa.PrivateKey, *x509.Certificate, []byte, error) {
	rootKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate random key, %w", err)
	}

	rootCertTmpl, err := createCACertTemplate(org, hosts, notAfter)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate CA cert, %w", err)
	}

	rootCert, rootCertPEM, err := createCert(rootCertTmpl, rootCertTmpl, &rootKey.PublicKey, rootKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to sign CA cert, %w", err)
	}
	return rootKey, rootCert, rootCertPEM, nil
}

// CreateCerts creates and returns a CA certificate and certificate and key
// if server==true, generate these for a server
// if client==true, generate these for a client
// can generate for both server and client but at least one must be specified
func CreateCerts(org string, hosts []string, notAfter time.Time, server bool, client bool) (serverKey, serverCert, caCert []byte, err error) {
	if !server && !client {
		return nil, nil, nil, fmt.Errorf("CreateCerts() must specify either server or client")
	}

	// Create a CA certificate and private key
	caKey, caCertificate, caCertificatePEM, err := createCA(org, hosts, notAfter)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create the private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate random key, %w", err)
	}
	var cert *x509.Certificate

	cert, err = certTemplate(org, hosts, notAfter)
	if err != nil {
		return nil, nil, nil, err
	}
	cert.KeyUsage = x509.KeyUsageDigitalSignature
	if server {
		cert.ExtKeyUsage = append(cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
	}
	if client {
		cert.ExtKeyUsage = append(cert.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	}

	// create a certificate wrapping the public key, sign it with the CA private key
	_, certPEM, err := createCert(cert, caCertificate, &privateKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to sign server cert, %w", err)
	}
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	return privateKeyPEM, certPEM, caCertificatePEM, nil
}
