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
	"encoding/base64"
	"encoding/json"
	"strconv"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// GetIsbSvcEnvVars is helper function to get the ISB service type and generate corresponnding environment variables
func GetIsbSvcEnvVars(isbSvcConfig dfv1.BufferServiceConfig) (dfv1.ISBSvcType, []corev1.EnvVar) {
	isbSvcConfigBytes, _ := json.Marshal(isbSvcConfig)
	encodedISBSvcConfig := base64.StdEncoding.EncodeToString(isbSvcConfigBytes)
	env := []corev1.EnvVar{
		{
			Name:  dfv1.EnvISBSvcConfig,
			Value: encodedISBSvcConfig,
		},
	}
	isbSvcType := dfv1.ISBSvcTypeUnknown
	if x := isbSvcConfig.JetStream; x != nil {
		env = append(env, corev1.EnvVar{Name: dfv1.EnvISBSvcJetStreamURL, Value: x.URL})
		env = append(env, corev1.EnvVar{Name: dfv1.EnvISBSvcJetStreamTLSEnabled, Value: strconv.FormatBool(x.TLSEnabled)})
		if x.Auth != nil && x.Auth.Basic != nil && x.Auth.Basic.User != nil && x.Auth.Basic.Password != nil {
			env = append(env, corev1.EnvVar{Name: dfv1.EnvISBSvcJetStreamUser, ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: x.Auth.Basic.User.Name,
					},
					Key: x.Auth.Basic.User.Key,
				},
			}})
			env = append(env, corev1.EnvVar{Name: dfv1.EnvISBSvcJetStreamPassword, ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: x.Auth.Basic.Password.Name,
					},
					Key: x.Auth.Basic.Password.Key,
				},
			}})
		}
		isbSvcType = dfv1.ISBSvcTypeJetStream
	}
	return isbSvcType, env
}
