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
	"github.com/Shopify/sarama"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"os"
)

// A utility function to get sasl.gssapi.Config
func GetGSSAPIConfig(config *dfv1.GSSAPI) (*sarama.GSSAPIConfig, error) {
	if config == nil {
		return nil, nil
	}

	c := &sarama.GSSAPIConfig{
		ServiceName: config.ServiceName,
		Username:    config.Username,
		Realm:       config.Realm,
	}

	switch *config.AuthType {
	case dfv1.KRB5UserAuth:
		c.AuthType = sarama.KRB5_USER_AUTH
	case dfv1.KRB5KeytabAuth:
		c.AuthType = sarama.KRB5_KEYTAB_AUTH
	default:
		return nil, fmt.Errorf("failed to parse GSSAPI AuthType %v. Must be one of the following: ['KRB5_USER_AUTH', 'KRB5_KEYTAB_AUTH']", config.AuthType)
	}

	if config.KeytabSecret != nil {
		keyTabPath, err := GetSecretVolumePath(config.KeytabSecret)
		if err != nil {
			return nil, err
		}
		if len(keyTabPath) > 0 {
			_, err := os.ReadFile(keyTabPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read keytab file %s, %w", keyTabPath, err)
			}
		}
		c.KeyTabPath = keyTabPath
	}

	if config.KerberosConfigSecret != nil {
		kerberosConfigPath, err := GetSecretVolumePath(config.KerberosConfigSecret)
		if err != nil {
			return nil, err
		}
		if len(kerberosConfigPath) > 0 {
			_, err := os.ReadFile(kerberosConfigPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read kerberos config file %s, %w", kerberosConfigPath, err)
			}
		}
		c.KerberosConfigPath = kerberosConfigPath
	}

	if config.PasswordSecret != nil {
		password, err := GetSecretFromVolume(config.PasswordSecret)
		if err != nil {
			return nil, err
		} else {
			c.Password = password
		}
	}

	return c, nil
}
