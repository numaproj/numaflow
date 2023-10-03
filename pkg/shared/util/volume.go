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
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

var (
	secretKeySelectorType    = reflect.TypeOf(&corev1.SecretKeySelector{})
	configMapKeySelectorType = reflect.TypeOf(&corev1.ConfigMapKeySelector{})
)

// GetSecretFromVolume retrieves the value of mounted secret volume
// "/var/numaflow/secrets/${secretRef.name}/${secretRef.key}" is expected to be the file path
func GetSecretFromVolume(selector *corev1.SecretKeySelector) (string, error) {
	filePath, err := GetSecretVolumePath(selector)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to get secret value of name: %s, key: %s, %w", selector.Name, selector.Key, err)
	}
	// Secrets edited by tools like "vim" always have an extra invisible "\n" in the end,
	// and it's often neglected, but it makes differences for some of the applications.
	return strings.TrimSuffix(string(data), "\n"), nil
}

// GetSecretVolumePath returns the path of the mounted secret
func GetSecretVolumePath(selector *corev1.SecretKeySelector) (string, error) {
	if selector == nil {
		return "", fmt.Errorf("secret key selector is nil")
	}
	return fmt.Sprintf("/var/numaflow/secrets/%s/%s", selector.Name, selector.Key), nil
}

// GetConfigMapFromVolume retrieves the value of mounted config map volume
// "/var/numaflow/config/${configMapRef.name}/${configMapRef.key}" is expected to be the file path
func GetConfigMapFromVolume(selector *corev1.ConfigMapKeySelector) (string, error) {
	filePath, err := GetConfigMapVolumePath(selector)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to get configMap value of name: %s, key: %s, %w", selector.Name, selector.Key, err)
	}
	// Contents edied by tools like "vim" always have an extra invisible "\n" in the end,
	// and it's often negleted, but it makes differences for some of the applications.
	return strings.TrimSuffix(string(data), "\n"), nil
}

// GetConfigMapVolumePath returns the path of the mounted configmap
func GetConfigMapVolumePath(selector *corev1.ConfigMapKeySelector) (string, error) {
	if selector == nil {
		return "", fmt.Errorf("configmap key selector is nil")
	}
	return fmt.Sprintf("/var/numaflow/config/%s/%s", selector.Name, selector.Key), nil
}

// VolumesFromSecretsOrConfigMaps builds volumes and volumeMounts spec based on
// the obj and its children's secretKeyselector and configMapKeySelector
func VolumesFromSecretsAndConfigMaps(obj interface{}) ([]corev1.Volume, []corev1.VolumeMount) {
	v := []corev1.Volume{}
	vm := []corev1.VolumeMount{}
	volSecrets, volSecretMounts := volumesFromSecretsOrConfigMaps(obj, secretKeySelectorType)
	v = append(v, volSecrets...)
	vm = append(vm, volSecretMounts...)
	volConfigMaps, volCofigMapMounts := volumesFromSecretsOrConfigMaps(obj, configMapKeySelectorType)
	v = append(v, volConfigMaps...)
	vm = append(vm, volCofigMapMounts...)
	return v, vm
}

func volumesFromSecretsOrConfigMaps(obj interface{}, t reflect.Type) ([]corev1.Volume, []corev1.VolumeMount) {
	resultVolumes := []corev1.Volume{}
	resultMounts := []corev1.VolumeMount{}
	values := findTypeValues(obj, t)
	if len(values) == 0 {
		return resultVolumes, resultMounts
	}
	switch t {
	case secretKeySelectorType:
		for _, v := range values {
			selector := v.(*corev1.SecretKeySelector)
			vol, mount := generateSecretVolumeSpecs(selector)
			resultVolumes = append(resultVolumes, vol)
			resultMounts = append(resultMounts, mount)
		}
	case configMapKeySelectorType:
		for _, v := range values {
			selector := v.(*corev1.ConfigMapKeySelector)
			vol, mount := generateConfigMapVolumeSpecs(selector)
			resultVolumes = append(resultVolumes, vol)
			resultMounts = append(resultMounts, mount)
		}
	default:
	}
	return uniqueVolumes(resultVolumes), uniqueVolumeMounts(resultMounts)
}

// Find all the values obj's children matching provided type, type needs to be a pointer
func findTypeValues(obj interface{}, t reflect.Type) []interface{} {
	result := []interface{}{}
	value := reflect.ValueOf(obj)
	findTypesRecursive(&result, value, t)
	return result
}

func findTypesRecursive(result *[]interface{}, obj reflect.Value, t reflect.Type) {
	if obj.Type() == t && obj.CanInterface() && !obj.IsNil() {
		*result = append(*result, obj.Interface())
	}
	switch obj.Kind() {
	case reflect.Ptr:
		objValue := obj.Elem()
		// Check if it is nil
		if !objValue.IsValid() {
			return
		}
		findTypesRecursive(result, objValue, t)
	case reflect.Interface:
		objValue := obj.Elem()
		// Check if it is nil
		if !objValue.IsValid() {
			return
		}
		findTypesRecursive(result, objValue, t)
	case reflect.Struct:
		for i := 0; i < obj.NumField(); i++ {
			if obj.Field(i).CanInterface() {
				findTypesRecursive(result, obj.Field(i), t)
			}
		}
	case reflect.Slice:
		for i := 0; i < obj.Len(); i++ {
			findTypesRecursive(result, obj.Index(i), t)
		}
	case reflect.Map:
		iter := obj.MapRange()
		for iter.Next() {
			findTypesRecursive(result, iter.Value(), t)
		}
	default:
		return
	}
}

// generateSecretVolumeSpecs builds a "volume" and "volumeMount"spec with a secretKeySelector
func generateSecretVolumeSpecs(selector *corev1.SecretKeySelector) (corev1.Volume, corev1.VolumeMount) {
	volName := strings.ReplaceAll("secret-"+selector.Name, "_", "-")
	return corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: selector.Name,
				},
			},
		}, corev1.VolumeMount{
			Name:      volName,
			ReadOnly:  true,
			MountPath: "/var/numaflow/secrets/" + selector.Name,
		}
}

// generateConfigMapVolumeSpecs builds a "volume" and "volumeMount"spec with a configMapKeySelector
func generateConfigMapVolumeSpecs(selector *corev1.ConfigMapKeySelector) (corev1.Volume, corev1.VolumeMount) {
	volName := strings.ReplaceAll("cm-"+selector.Name, "_", "-")
	return corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: selector.Name,
					},
				},
			},
		}, corev1.VolumeMount{
			Name:      volName,
			ReadOnly:  true,
			MountPath: "/var/numaflow/config/" + selector.Name,
		}
}

func uniqueVolumes(vols []corev1.Volume) []corev1.Volume {
	rVols := []corev1.Volume{}
	keys := make(map[string]bool)
	for _, e := range vols {
		if _, value := keys[e.Name]; !value {
			keys[e.Name] = true
			rVols = append(rVols, e)
		}
	}
	return rVols
}

func uniqueVolumeMounts(mounts []corev1.VolumeMount) []corev1.VolumeMount {
	rMounts := []corev1.VolumeMount{}
	keys := make(map[string]bool)
	for _, e := range mounts {
		if _, value := keys[e.Name]; !value {
			keys[e.Name] = true
			rMounts = append(rMounts, e)
		}
	}
	return rMounts
}
