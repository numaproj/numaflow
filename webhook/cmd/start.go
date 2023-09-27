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
	"crypto/tls"
	"os"
	"strconv"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	"github.com/numaproj/numaflow"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/webhook"
)

const (
	serviceNameEnvVar     = "SERVICE_NAME"
	deploymentNameEnvVar  = "DEPLOYMENT_NAME"
	clusterRoleNameEnvVar = "CLUSTER_ROLE_NAME"
	namespaceEnvVar       = "NAMESPACE"
	portEnvVar            = "PORT"
)

func Start() {
	logger := logging.NewLogger().Named("webhook")
	var restConfig *rest.Config
	var err error
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, _ := os.UserHomeDir()
		kubeconfig = home + "/.kube/config"
		if _, err := os.Stat(kubeconfig); err != nil && os.IsNotExist(err) {
			kubeconfig = ""
		}
	}
	if kubeconfig != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		restConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		logger.Fatalw("Failed to get kubeconfig", zap.Error(err))
	}

	namespace, defined := os.LookupEnv(namespaceEnvVar)
	if !defined {
		logger.Fatal("Required namespace variable isn't set")
	}

	kubeClient := kubernetes.NewForConfigOrDie(restConfig)
	numaClient := versioned.NewForConfigOrDie(restConfig).NumaflowV1alpha1()
	isbSvcClient := numaClient.InterStepBufferServices(namespace)

	portStr := sharedutil.LookupEnvStringOr(portEnvVar, "443")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		logger.Fatal("Port should be a number, not valid")
	}

	options := webhook.Options{
		ServiceName:     sharedutil.LookupEnvStringOr(serviceNameEnvVar, "numaflow-webhook"),
		DeploymentName:  sharedutil.LookupEnvStringOr(deploymentNameEnvVar, "numaflow-webhook"),
		ClusterRoleName: sharedutil.LookupEnvStringOr(clusterRoleNameEnvVar, "numaflow-webhook"),
		Namespace:       namespace,
		Port:            port,
		SecretName:      "numaflow-webhook-certs",
		WebhookName:     "webhook.numaflow.numaproj.io",
		ClientAuth:      tls.VerifyClientCertIfGiven,
	}
	controller := webhook.AdmissionController{
		Client:       kubeClient,
		NumaClient:   numaClient,
		ISBSVCClient: isbSvcClient,
		Options:      options,
		Handlers: map[schema.GroupVersionKind]runtime.Object{
			{Group: "numaflow.numaproj.io", Version: "v1alpha1", Kind: "InterStepBufferService"}: &dfv1.InterStepBufferService{},
			{Group: "numaflow.numaproj.io", Version: "v1alpha1", Kind: "Pipeline"}:               &dfv1.Pipeline{},
		},
		Logger: logger,
	}
	logger.Infow("Starting admission controller", "version", numaflow.GetVersion())
	ctx := logging.WithLogger(signals.SetupSignalHandler(), logger)
	if err := controller.Run(ctx); err != nil {
		logger.Fatalw("Failed to create admission controller", zap.Error(err))
	}
}
