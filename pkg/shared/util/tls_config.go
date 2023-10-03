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

package util

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// A utility function to get tls.Config
func GetTLSConfig(config *dfv1.TLS) (*tls.Config, error) {
	if config == nil {
		return nil, nil
	}

	var caCertPath, certPath, keyPath string
	var err error
	if config.CACertSecret != nil {
		caCertPath, err = GetSecretVolumePath(config.CACertSecret)
		if err != nil {
			return nil, err
		}
	}

	if config.CertSecret != nil {
		certPath, err = GetSecretVolumePath(config.CertSecret)
		if err != nil {
			return nil, err
		}
	}

	if config.KeySecret != nil {
		keyPath, err = GetSecretVolumePath(config.KeySecret)
		if err != nil {
			return nil, err
		}
	}

	if len(certPath)+len(keyPath) > 0 && len(certPath)*len(keyPath) == 0 {
		// Only one of certSecret and keySecret is configured
		return nil, fmt.Errorf("invalid tls config, both certSecret and keySecret need to be configured")
	}

	c := &tls.Config{
		InsecureSkipVerify: config.InsecureSkipVerify,
	}
	if len(caCertPath) > 0 {
		caCert, err := os.ReadFile(caCertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca cert file %s, %w", caCertPath, err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caCert)
		c.RootCAs = pool
	}

	if len(certPath) > 0 && len(keyPath) > 0 {
		clientCert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert key pair (%s, %s), %w", certPath, keyPath, err)
		}
		c.Certificates = []tls.Certificate{clientCert}
	}
	return c, nil
}
