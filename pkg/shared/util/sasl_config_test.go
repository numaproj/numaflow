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
	"testing"

	"github.com/IBM/sarama"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestSaslConfiguration(t *testing.T) {
	mockedVolumes := mockedVolumes{
		volumeSecrets: map[struct {
			objectName string
			key        string
		}]string{
			{
				objectName: "user-secret-name",
				key:        "user",
			}: "user",
			{
				objectName: "password-secret-name",
				key:        "password",
			}: "password",
		},
	}

	credentials := &dfv1.SASLPlain{
		UserSecret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: "user-secret-name",
			},
			Key: "user",
		},
		PasswordSecret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: "password-secret-name",
			},
			Key: "password",
		},
		Handshake: true,
	}

	t.Run("Plain produces right values", func(t *testing.T) {
		plain := dfv1.SASLTypePlaintext
		config, err := getSASLStrategy(&dfv1.SASL{
			Mechanism: &plain,
			Plain:     credentials,
		}, mockedVolumes)
		assert.NoError(t, err)
		assert.Equal(t, true, config.Enable)
		assert.Equal(t, sarama.SASLTypePlaintext, string(config.Mechanism))
		assert.Equal(t, true, config.Handshake)
		assert.Equal(t, "user", config.User)
		assert.Equal(t, "password", config.Password)
	})

	t.Run("SCRAM SHA 256 produces right values", func(t *testing.T) {
		sasl_256 := dfv1.SASLTypeSCRAMSHA256
		config, err := getSASLStrategy(&dfv1.SASL{
			Mechanism:   &sasl_256,
			SCRAMSHA256: credentials,
		}, mockedVolumes)
		assert.NoError(t, err)
		assert.Equal(t, true, config.Enable)
		assert.Equal(t, sarama.SASLTypeSCRAMSHA256, string(config.Mechanism))
		assert.Equal(t, true, config.Handshake)
		assert.Equal(t, "user", config.User)
		assert.Equal(t, "password", config.Password)
	})

	t.Run("SCRAM SHA 512 produces right values", func(t *testing.T) {
		sasl_512 := dfv1.SASLTypeSCRAMSHA512
		config, err := getSASLStrategy(&dfv1.SASL{
			Mechanism:   &sasl_512,
			SCRAMSHA512: credentials,
		}, mockedVolumes)
		assert.NoError(t, err)
		assert.Equal(t, true, config.Enable)
		assert.Equal(t, sarama.SASLTypeSCRAMSHA512, string(config.Mechanism))
		assert.Equal(t, true, config.Handshake)
		assert.Equal(t, "user", config.User)
		assert.Equal(t, "password", config.Password)
	})
}

// func TestGetGSSAPIConfig(t *testing.T) {
// 	t.Run("Basic case: Return GSSAPIConfig for valid input", func(t *testing.T) {
// 		authType := dfv1.KRB5UserAuth
// 		config := &dfv1.GSSAPI{
// 			ServiceName: "kafka",
// 			Realm:       "EXAMPLE.COM",
// 			AuthType:    &authType,
// 			UsernameSecret: &corev1.SecretKeySelector{
// 				LocalObjectReference: corev1.LocalObjectReference{
// 					Name: "myUsername",
// 				},
// 				Key: "user",
// 			},
// 			PasswordSecret: &corev1.SecretKeySelector{
// 				LocalObjectReference: corev1.LocalObjectReference{
// 					Name: "myPassword",
// 				},
// 				Key: "password",
// 			},
// 		}

// 		result, err := GetGSSAPIConfig(config)
// 		require.NoError(t, err)
// 		assert.NotNil(t, result)
// 		assert.Equal(t, "kafka", result.ServiceName)
// 		assert.Equal(t, "EXAMPLE.COM", result.Realm)
// 		assert.Equal(t, sarama.KRB5_USER_AUTH, result.AuthType)
// 		assert.Equal(t, "myUsername", result.Username)
// 		assert.Equal(t, "myPassword", result.Password)
// 	})

// 	t.Run("Error case: Invalid AuthType", func(t *testing.T) {
// 		config := &dfv1.GSSAPI{
// 			ServiceName: "kafka",
// 			Realm:       "EXAMPLE.COM",
// 			AuthType:    nil,
// 		}

// 		result, err := GetGSSAPIConfig(config)
// 		assert.Nil(t, result)
// 		assert.Error(t, err)
// 		assert.Contains(t, err.Error(), "failed to parse GSSAPI AuthType")
// 	})

// 	t.Run("Error case: Fetching secret key failed", func(t *testing.T) {
// 		authType := dfv1.KRB5UserAuth
// 		config := &dfv1.GSSAPI{
// 			ServiceName: "kafka",
// 			Realm:       "EXAMPLE.COM",
// 			AuthType:    &authType,
// 			UsernameSecret: &corev1.SecretKeySelector{
// 				LocalObjectReference: corev1.LocalObjectReference{
// 					Name: "user-secret-name",
// 				},
// 				Key: "user",
// 			},
// 		}

// 		result, err := GetGSSAPIConfig(config)
// 		assert.Nil(t, result)
// 		assert.Error(t, err)
// 		assert.Contains(t, err.Error(), "secret not found")
// 	})

// 	t.Run("Error case: Reading keytab file fails", func(t *testing.T) {
// 		authType := dfv1.KRB5KeytabAuth
// 		config := &dfv1.GSSAPI{
// 			ServiceName: "kafka",
// 			Realm:       "EXAMPLE.COM",
// 			AuthType:    &authType,
// 			KeytabSecret: &corev1.SecretKeySelector{
// 				LocalObjectReference: corev1.LocalObjectReference{
// 					Name: "KeytabSecret",
// 				},
// 				Key: "keytab",
// 			},
// 		}

// 		result, err := GetGSSAPIConfig(config)
// 		assert.Nil(t, result)
// 		assert.Error(t, err)
// 		assert.Contains(t, err.Error(), "failed to read keytab file")
// 	})

// 	t.Run("Error case: Reading Kerberos config file fails", func(t *testing.T) {
// 		authType := dfv1.KRB5UserAuth
// 		config := &dfv1.GSSAPI{
// 			ServiceName: "kafka",
// 			Realm:       "EXAMPLE.COM",
// 			AuthType:    &authType,
// 			KerberosConfigSecret: &corev1.SecretKeySelector{
// 				LocalObjectReference: corev1.LocalObjectReference{
// 					Name: "ValidKerberosConfigSecret",
// 				},
// 				Key: "KerberosConfigSecret",
// 			},
// 		}

// 		result, err := GetGSSAPIConfig(config)
// 		assert.Nil(t, result)
// 		assert.Error(t, err)
// 		assert.Contains(t, err.Error(), "failed to read kerberos config file")
// 	})
// }
