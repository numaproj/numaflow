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

	t.Run("gssapi", func(t *testing.T) {
		plain := dfv1.SASLTypePlaintext
		temp := dfv1.SASL{
			Mechanism: &plain,
			Plain:     credentials,
		}
		config, err := GetGSSAPIConfig(temp.GSSAPI)
		assert.NoError(t, err)
		assert.Nil(t, config)

	})
}

func TestGetGSSAPIConfig_NilConfig(t *testing.T) {
	config, err := GetGSSAPIConfig(nil)
	assert.NoError(t, err)
	assert.Nil(t, config)
}

func TestGetGSSAPIConfig_InvalidAuthType(t *testing.T) {

	var authType dfv1.KRB5AuthType = "anytpe"

	config := &dfv1.GSSAPI{
		ServiceName: "service",
		Realm:       "realm",
		AuthType:    &authType,
	}

	_, err := GetGSSAPIConfig(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse GSSAPI AuthType")
}

func TestXDGSCRAMClient_Begin_SHA256(t *testing.T) {
	client := &XDGSCRAMClient{HashGeneratorFcn: SHA256}
	err := client.Begin("username", "password", "")
	assert.NoError(t, err)
	assert.NotNil(t, client.Client)
	assert.NotNil(t, client.ClientConversation)
}

func TestXDGSCRAMClient_Begin_SHA512(t *testing.T) {
	client := &XDGSCRAMClient{HashGeneratorFcn: SHA512}
	err := client.Begin("username", "password", "")
	assert.NoError(t, err)
	assert.NotNil(t, client.Client)
	assert.NotNil(t, client.ClientConversation)
}

func TestXDGSCRAMClient_Step(t *testing.T) {
	client := &XDGSCRAMClient{HashGeneratorFcn: SHA256}
	err := client.Begin("username", "password", "")
	assert.NoError(t, err)

	response, err := client.Step("challenge")
	assert.NoError(t, err)
	assert.NotEmpty(t, response)
}

func TestXDGSCRAMClient_Done(t *testing.T) {
	client := &XDGSCRAMClient{HashGeneratorFcn: SHA256}
	err := client.Begin("username", "password", "")
	assert.NoError(t, err)

	_, err = client.Step("challenge")
	assert.NoError(t, err)
	assert.False(t, client.Done())
}

type mockGSSAPI struct {
	ServiceName          string
	Realm                string
	UsernameSecret       *corev1.SecretKeySelector
	AuthType             *dfv1.KRB5AuthType
	PasswordSecret       *corev1.SecretKeySelector
	KeytabSecret         *corev1.SecretKeySelector
	KerberosConfigSecret *corev1.SecretKeySelector
}

func TestGetGSSAPIConfig(t *testing.T) {

	authType := dfv1.KRB5UserAuth
	tests := []struct {
		name    string
		config  *mockGSSAPI
		want    *sarama.GSSAPIConfig
		wantErr bool
	}{
		{
			name: "invalid auth type",
			config: &mockGSSAPI{
				ServiceName: "testService",
				Realm:       "testRealm",
				AuthType:    new(dfv1.KRB5AuthType), // invalid auth type
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error fetching username secret",
			config: &mockGSSAPI{
				ServiceName: "testService",
				Realm:       "testRealm",
				AuthType:    &authType,
				UsernameSecret: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "error"},
					Key:                  "username",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error fetching Kerbos config secret",
			config: &mockGSSAPI{
				ServiceName: "testService",
				Realm:       "testRealm",
				AuthType:    &authType,
				KeytabSecret: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "error"},
					Key:                  "keytab",
				},
			},
			want:    nil,
			wantErr: true,
		},

		{
			name: "error fetching keytab file",
			config: &mockGSSAPI{
				ServiceName: "testService",
				Realm:       "testRealm",
				AuthType:    &authType,
				KerberosConfigSecret: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "error"},
					Key:                  "KerberosConfig",
				},
			},
			want:    nil,
			wantErr: true,
		},

		{
			name: "error fetching password",
			config: &mockGSSAPI{
				ServiceName: "testService",
				Realm:       "testRealm",
				AuthType:    &authType,
				PasswordSecret: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "error"},
					Key:                  "PasswordS",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetGSSAPIConfig(&dfv1.GSSAPI{
				ServiceName:          tt.config.ServiceName,
				Realm:                tt.config.Realm,
				UsernameSecret:       tt.config.UsernameSecret,
				AuthType:             tt.config.AuthType,
				PasswordSecret:       tt.config.PasswordSecret,
				KeytabSecret:         tt.config.KeytabSecret,
				KerberosConfigSecret: tt.config.KerberosConfigSecret,
			})

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
