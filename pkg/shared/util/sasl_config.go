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
	"fmt"
	"os"

	"github.com/Shopify/sarama"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// A utility function to get sasl.gssapi.Config
func GetGSSAPIConfig(config *dfv1.SASL) (*sarama.GSSAPIConfig, error) {
	if config == nil {
		return nil, nil
	}

	c := &sarama.GSSAPIConfig{
		AuthType:    int(config.GSSAPIAuthType),
		ServiceName: config.GSSAPIServiceName,
		Username:    config.GSSAPIUsername,
		Realm:       config.GSSAPIRealm,
	}

	if config.KeyTabSecretSecret != nil {
		gssapiKeyTabPath, err := GetSecretVolumePath(config.KeyTabSecretSecret)
		if err != nil {
			return nil, err
		}
		if len(gssapiKeyTabPath) > 0 {
			_, err := os.ReadFile(gssapiKeyTabPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read keytab file %s, %w", gssapiKeyTabPath, err)
			}
		}
		c.KeyTabPath = gssapiKeyTabPath
	}

	if config.KerberosConfigSecret != nil {
		gssapiKerberosConfigPath, err := GetSecretVolumePath(config.KerberosConfigSecret)
		if err != nil {
			return nil, err
		}
		if len(gssapiKerberosConfigPath) > 0 {
			_, err := os.ReadFile(gssapiKerberosConfigPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read kerberos config file %s, %w", gssapiKerberosConfigPath, err)
			}
		}
		c.KerberosConfigPath = gssapiKerberosConfigPath
	}

	if config.GSSAPIPasswordSecret != nil {
		gssapiPassword, err := GetSecretFromVolume(config.GSSAPIPasswordSecret)
		if err != nil {
			return nil, err
		} else {
			c.Password = gssapiPassword
		}
	}

	return c, nil
}
