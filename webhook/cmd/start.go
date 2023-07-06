package cmd

import (
	"crypto/tls"
	"os"
	"strconv"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/webhook"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
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
		logger.Fatalf("", zap.Error(err))
	}

	namespace, defined := os.LookupEnv(namespaceEnvVar)
	if !defined {
		logger.Fatal("namespace variable")
	}

	kubeClient := kubernetes.NewForConfigOrDie(restConfig)
	isbSvcClient := versioned.NewForConfigOrDie(restConfig).NumaflowV1alpha1().InterStepBufferServices(namespace)

	portStr := sharedutil.LookupEnvStringOr(portEnvVar, "443")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		logger.Fatal("port var")
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
		ISBSVCClient: isbSvcClient,
		Options:      options,
		Handlers: map[schema.GroupVersionKind]runtime.Object{
			{Group: "numaflow.numaproj.io", Version: "v1alpha1", Kind: "InterStepBufferService"}: &dfv1.InterStepBufferService{},
		},
		Logger: logger,
	}
	ctx := logging.WithLogger(signals.SetupSignalHandler(), logger)
	if err := controller.Run(ctx); err != nil {
		logger.Fatalw("", zap.Error(err))
	}

}
