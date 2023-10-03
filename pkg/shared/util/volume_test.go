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

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

var (
	testSecretKeySelector = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: "test-secret",
		},
		Key: "test-key",
	}

	testConfigMapSelector = &corev1.ConfigMapKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: "test-cm",
		},
		Key: "test-key",
	}
)

func Test_generateSecretVolumeSpecs(t *testing.T) {
	v, vm := generateSecretVolumeSpecs(testSecretKeySelector)
	assert.NotNil(t, v.VolumeSource.Secret)
	assert.Equal(t, "test-secret", v.VolumeSource.Secret.SecretName)
	assert.Equal(t, v.Name, vm.Name)
}

func Test_generateConfigMapVolumeSpecs(t *testing.T) {
	v, vm := generateConfigMapVolumeSpecs(testConfigMapSelector)
	assert.NotNil(t, v.VolumeSource.ConfigMap)
	assert.Equal(t, "test-cm", v.VolumeSource.ConfigMap.Name)
	assert.Equal(t, v.Name, vm.Name)
}

func Test_uniqueVolumes_VolumeMounts(t *testing.T) {
	v1, vm1 := generateSecretVolumeSpecs(testSecretKeySelector)
	v2, vm2 := generateConfigMapVolumeSpecs(testConfigMapSelector)
	assert.Equal(t, 2, len(uniqueVolumes([]corev1.Volume{v1, v2})))
	assert.Equal(t, 2, len(uniqueVolumeMounts([]corev1.VolumeMount{vm1, vm2})))
	assert.Equal(t, 2, len(uniqueVolumes([]corev1.Volume{v1, v2, v1})))
	assert.Equal(t, 2, len(uniqueVolumeMounts([]corev1.VolumeMount{vm1, vm2, vm1})))
}

func Test_volumesFromSecretsOrConfigMaps(t *testing.T) {
	t.Run("test secret", func(t *testing.T) {
		m := map[string]*corev1.SecretKeySelector{
			"a": testSecretKeySelector,
			"b": testSecretKeySelector,
			"c": {
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "test-secret-1",
				},
				Key: "test-key",
			},
		}
		vs, vms := volumesFromSecretsOrConfigMaps(m, secretKeySelectorType)
		assert.Equal(t, 2, len(vs))
		assert.Equal(t, 2, len(vms))
	})

	t.Run("test config map", func(t *testing.T) {
		m := map[string]*corev1.ConfigMapKeySelector{
			"a": testConfigMapSelector,
			"b": testConfigMapSelector,
			"c": {
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "test-cm-1",
				},
				Key: "test-key",
			},
		}
		vs, vms := volumesFromSecretsOrConfigMaps(m, configMapKeySelectorType)
		assert.Equal(t, 2, len(vs))
		assert.Equal(t, 2, len(vms))
	})

	t.Run("test both", func(t *testing.T) {
		m := map[string]interface{}{
			"a1": testSecretKeySelector,
			"b1": testSecretKeySelector,
			"c1": &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "test-secret-1",
				},
				Key: "test-key",
			},
			"a2": testConfigMapSelector,
			"b2": testConfigMapSelector,
			"c2": &corev1.ConfigMapKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "test-cm-1",
				},
				Key: "test-key",
			},
		}
		vs, vms := VolumesFromSecretsAndConfigMaps(m)
		assert.Equal(t, 4, len(vs))
		assert.Equal(t, 4, len(vms))
	})
}

func Test_GetConfigMapVolumePath(t *testing.T) {
	p, e := GetConfigMapVolumePath(testConfigMapSelector)
	assert.Nil(t, e)
	assert.Equal(t, "/var/numaflow/config/test-cm/test-key", p)
}

func Test_GetSecretVolumePath(t *testing.T) {
	p, e := GetSecretVolumePath(testSecretKeySelector)
	assert.Nil(t, e)
	assert.Equal(t, "/var/numaflow/secrets/test-secret/test-key", p)
}
