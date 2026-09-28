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

func TestGetJSIsbSvcEnvVars(t *testing.T) {
	fakeIsbsConfig := dfv1.BufferServiceConfig{
		JetStream: &dfv1.JetStreamConfig{
			URL:          "xxx",
			TLSEnabled:   false,
			StreamConfig: "",
			Auth: &dfv1.NatsAuth{
				Basic: &dfv1.BasicAuth{
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

// The dataplane selects its ISB backend from NUMAFLOW_ISBSVC_TYPE.
// The type must therefore be emitted as a pod env var, not only as the
// --isbsvc-type container arg.
func TestGetIsbSvcEnvVarsEmitsISBSvcType(t *testing.T) {
	t.Run("jetstream config emits the type", func(t *testing.T) {
		cfg := dfv1.BufferServiceConfig{JetStream: &dfv1.JetStreamConfig{URL: "nats://x:4222"}}
		tp, env := GetIsbSvcEnvVars(cfg)
		assert.Equal(t, dfv1.ISBSvcTypeJetStream, tp)
		envMap := map[string]string{}
		for _, e := range env {
			envMap[e.Name] = e.Value
		}
		v, ok := envMap["NUMAFLOW_ISBSVC_TYPE"]
		assert.True(t, ok, "NUMAFLOW_ISBSVC_TYPE must be emitted")
		assert.Equal(t, "jetstream", v)
	})

	t.Run("empty config emits no type", func(t *testing.T) {
		tp, env := GetIsbSvcEnvVars(dfv1.BufferServiceConfig{})
		assert.Equal(t, dfv1.ISBSvcTypeUnknown, tp)
		for _, e := range env {
			assert.NotEqual(t, "NUMAFLOW_ISBSVC_TYPE", e.Name)
		}
	})
}
