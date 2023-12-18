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

package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/numaproj/numaflow/pkg/shared/util"
)

const (
	namespace       = "numaflow-system"
	secret          = "numaflow-server-secrets"
	serverSecretKey = "server.secretkey"
	passwordKey     = "admin.initial-password"
)

func Start() error {
	var (
		k8sRestConfig     *rest.Config
		err               error
		secretKeyExists   bool
		passwordKeyExists bool
		secretMap         = map[string][]byte{}
	)
	k8sRestConfig, err = util.K8sRestConfig()
	if err != nil {
		return fmt.Errorf("failed to get kubeRestConfig, %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(k8sRestConfig)
	if err != nil {
		return fmt.Errorf("failed to get kubeclient, %w", err)
	}

	k8sSecret, err := kubeClient.CoreV1().Secrets(namespace).Get(context.Background(), secret, metav1.GetOptions{})
	if err != nil {
		return err
	}

	_, secretKeyExists = k8sSecret.Data[serverSecretKey]
	if !secretKeyExists {
		secretKey := base64.URLEncoding.EncodeToString([]byte(util.RandomString(32)))
		secretMap[serverSecretKey] = []byte(secretKey) // base64 encoded secretKey
	}

	_, passwordKeyExists = k8sSecret.Data[passwordKey]
	if !passwordKeyExists {
		password := base64.URLEncoding.EncodeToString([]byte(util.RandomString(8)))
		secretMap[passwordKey] = []byte(password) // base64 encoded password
	}

	if !secretKeyExists || !passwordKeyExists {
		k8sSecret.Data = secretMap
		_, err = kubeClient.CoreV1().Secrets(namespace).Update(context.Background(), k8sSecret, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update k8s secret with admin password and secretKey, %w", err)
		}
	}

	return nil
}
