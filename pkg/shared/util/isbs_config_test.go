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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestGetRedisIsbSvcEnvVars(t *testing.T) {
	fakeIsbSvcConfig := dfv1.BufferServiceConfig{
		Redis: &dfv1.RedisConfig{
			URL:         "xxx",
			User:        "test-user",
			SentinelURL: "xxx",
			MasterName:  "master",
			Password: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "test-name",
				},
				Key: "test-key",
			},
			SentinelPassword: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "test-name",
				},
				Key: "test-key",
			},
		},
	}
	tp, env := GetIsbSvcEnvVars(fakeIsbSvcConfig)
	assert.Equal(t, dfv1.ISBSvcTypeRedis, tp)
	assert.True(t, len(env) > 0)
	eNames := []string{}
	eValues := []string{}
	for _, e := range env {
		eNames = append(eNames, e.Name)
		eValues = append(eValues, e.Value)
	}
	if fakeIsbSvcConfig.Redis.MasterName != "" {
		assert.Contains(t, eNames, dfv1.EnvISBSvcSentinelMaster)
	}
	if fakeIsbSvcConfig.Redis.Password != nil {
		assert.Contains(t, eNames, dfv1.EnvISBSvcRedisPassword)
	}
	if fakeIsbSvcConfig.Redis.SentinelPassword != nil {
		assert.Contains(t, eNames, dfv1.EnvISBSvcRedisSentinelPassword)
	}
	if fakeIsbSvcConfig.Redis.SentinelURL != "" {
		assert.Contains(t, eNames, dfv1.EnvISBSvcRedisSentinelURL)
	}
	assert.Contains(t, eNames, dfv1.EnvISBSvcRedisURL)
	assert.Contains(t, eValues, "test-user")
	assert.Contains(t, eValues, "xxx")
	assert.Contains(t, eNames, dfv1.EnvISBSvcConfig)
	assert.Contains(t, eNames, dfv1.EnvISBSvcSentinelMaster)
	assert.Contains(t, eNames, dfv1.EnvISBSvcRedisSentinelURL)
}

func TestGetJSIsbSvcEnvVars(t *testing.T) {
	fakeIsbsConfig := dfv1.BufferServiceConfig{
		JetStream: &dfv1.JetStreamConfig{
			URL:          "xxx",
			TLSEnabled:   false,
			BufferConfig: "",
			Auth: &dfv1.NATSAuth{
				User: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "test-user",
					},
					Key: "test-key",
				},
				Password: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "test-pass",
					},
					Key: "test-key",
				},
			},
		},
	}
	tp, env := GetIsbSvcEnvVars(fakeIsbsConfig)
	assert.Equal(t, dfv1.ISBSvcTypeJetStream, tp)
	eNames := []string{}
	for _, e := range env {
		eNames = append(eNames, e.Name)
	}
	assert.Contains(t, eNames, dfv1.EnvISBSvcJetStreamURL)
	assert.Contains(t, eNames, dfv1.EnvISBSvcJetStreamTLSEnabled)
	assert.Contains(t, eNames, dfv1.EnvISBSvcJetStreamUser)
	assert.Contains(t, eNames, dfv1.EnvISBSvcJetStreamPassword)
	assert.Contains(t, eNames, dfv1.EnvISBSvcConfig)
}
