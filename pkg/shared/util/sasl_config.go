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

	"github.com/IBM/sarama"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// GetSASLConfig A utility function to get sarama.Config.Net.SASL
func GetSASL(saslConfig *dfv1.SASL) (*struct {
	Enable                   bool
	Mechanism                sarama.SASLMechanism
	Version                  int16
	Handshake                bool
	AuthIdentity             string
	User                     string
	Password                 string
	SCRAMAuthzID             string
	SCRAMClientGeneratorFunc func() sarama.SCRAMClient
	TokenProvider            sarama.AccessTokenProvider
	GSSAPI                   sarama.GSSAPIConfig
}, error) {
	config := sarama.NewConfig()
	switch *saslConfig.Mechanism {
	case dfv1.SASLTypeGSSAPI:
		if gssapi := saslConfig.GSSAPI; gssapi != nil {
			config.Net.SASL.Enable = true
			config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
			if gssapi, err := GetGSSAPIConfig(gssapi); err != nil {
				return nil, fmt.Errorf("error loading gssapi config, %w", err)
			} else {
				config.Net.SASL.GSSAPI = *gssapi
			}
		}
	case dfv1.SASLTypePlaintext:
		if plain := saslConfig.Plain; plain != nil {
			config.Net.SASL.Enable = true
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			if plain.UserSecret != nil {
				user, err := GetSecretFromVolume(plain.UserSecret)
				if err != nil {
					return nil, err
				} else {
					config.Net.SASL.User = user
				}
			}
			if plain.PasswordSecret != nil {
				password, err := GetSecretFromVolume(plain.PasswordSecret)
				if err != nil {
					return nil, err
				} else {
					config.Net.SASL.Password = password
				}
			}
			config.Net.SASL.Handshake = plain.Handshake
		}
	default:
		return nil, fmt.Errorf("SASL mechanism not supported: %s", *saslConfig.Mechanism)
	}
	return &config.Net.SASL, nil
}

// GetGSSAPIConfig A utility function to get sasl.gssapi.Config
func GetGSSAPIConfig(config *dfv1.GSSAPI) (*sarama.GSSAPIConfig, error) {
	if config == nil {
		return nil, nil
	}

	c := &sarama.GSSAPIConfig{
		ServiceName: config.ServiceName,
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

	if config.UsernameSecret != nil {
		username, err := GetSecretFromVolume(config.UsernameSecret)
		if err != nil {
			return nil, err
		} else {
			c.Username = username
		}
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
