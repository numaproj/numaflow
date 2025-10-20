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

package fixtures

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	flowversiond "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	flowpkg "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

const (
	Namespace  = "numaflow-system"
	Label      = "numaflow-e2e"
	LabelValue = "true"
	ISBSvcName = "numaflow-e2e"
	// the number 120 is carefully chosen to ensure the test suite can finish within a reasonable time without timing out.
	// please exercise caution when updating this value, as it may cause e2e tests to be flaky.
	// if updated, consider running the entire e2e test suite multiple times to ensure stability.
	defaultTimeout = 120 * time.Second

	LogSourceVertexStarted    = "Start processing source messages"
	SinkVertexStarted         = "Start processing sink messages"
	LogUDFVertexStarted       = "Start processing udf messages"
	LogReduceUDFVertexStarted = "Start processing reduce udf messages"
	LogDaemonStarted          = "Daemon server started successfully"

	LogSourceVertexStartedRustRuntime = "Starting source forwarder"
	LogSinkVertexStartedRustRuntime   = "Starting sink forwarder"
	LogMapVertexStartedRustRuntime    = "Starting map forwarder"
)

var (
	background = metav1.DeletePropagationBackground

	e2eISBSvcJetStream = `apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
  labels:
    numaflow-e2e: "true"
spec:
  jetstream:
    version: latest
    persistence:
      volumeSize: 50Mi`
)

type E2ESuite struct {
	suite.Suite
	restConfig            *rest.Config
	isbSvcClient          flowpkg.InterStepBufferServiceInterface
	pipelineClient        flowpkg.PipelineInterface
	servingPipelineClient flowpkg.ServingPipelineInterface
	vertexClient          flowpkg.VertexInterface
	monoVertexClient      flowpkg.MonoVertexInterface
	kubeClient            kubernetes.Interface
	stopch                chan struct{}
}

func (s *E2ESuite) SetupSuite() {
	var err error
	s.restConfig, err = sharedutil.K8sRestConfig()
	s.stopch = make(chan struct{})
	s.CheckError(err)
	s.kubeClient, err = kubernetes.NewForConfig(s.restConfig)
	s.CheckError(err)
	s.isbSvcClient = flowversiond.NewForConfigOrDie(s.restConfig).NumaflowV1alpha1().InterStepBufferServices(Namespace)
	s.pipelineClient = flowversiond.NewForConfigOrDie(s.restConfig).NumaflowV1alpha1().Pipelines(Namespace)
	s.vertexClient = flowversiond.NewForConfigOrDie(s.restConfig).NumaflowV1alpha1().Vertices(Namespace)
	s.monoVertexClient = flowversiond.NewForConfigOrDie(s.restConfig).NumaflowV1alpha1().MonoVertices(Namespace)
	s.servingPipelineClient = flowversiond.NewForConfigOrDie(s.restConfig).NumaflowV1alpha1().ServingPipelines(Namespace)

	// Clean up resources if any
	s.deleteResources([]schema.GroupVersionResource{
		dfv1.PipelineGroupVersionResource,
		dfv1.ISBGroupVersionResource,
		batchv1.SchemeGroupVersion.WithResource("jobs"),
	})
	s.Given().ISBSvc(getISBSvcSpec()).
		When().
		Expect().
		ISBSvcDeleted(defaultTimeout)

	s.Given().ISBSvc(getISBSvcSpec()).
		When().
		CreateISBSvc().
		WaitForISBSvcReady()
	s.T().Log("ISB svc is ready")
	err = PodPortForward(s.restConfig, Namespace, "e2e-api-pod", 8378, 8378, s.stopch)
	s.CheckError(err)

	// Create Redis resources used for sink data validation.
	deleteCMD := fmt.Sprintf("kubectl delete -k ../../config/apps/redis -n %s --ignore-not-found=true", Namespace)
	s.Given().When().Exec("sh", []string{"-c", deleteCMD}, OutputRegexp(""))
	createCMD := fmt.Sprintf("kubectl apply -k ../../config/apps/redis -n %s", Namespace)
	s.Given().When().Exec("sh", []string{"-c", createCMD}, OutputRegexp("service/redis created"))
	s.T().Log("Redis resources are ready")
}

func (s *E2ESuite) TearDownSuite() {
	s.deleteResources([]schema.GroupVersionResource{
		dfv1.PipelineGroupVersionResource,
	})
	s.deleteResources([]schema.GroupVersionResource{
		batchv1.SchemeGroupVersion.WithResource("jobs"),
	})
	s.Given().ISBSvc(getISBSvcSpec()).
		When().
		Wait(5 * time.Second).
		DeleteISBSvc().
		Wait(3 * time.Second)
	// force deleting the ISB svc pods because we have seen pods stuck in terminating state after CRD deletion,
	// which causes e2e tests to timeout, this is a workaround to avoid the issue.
	deleteISBPodsCMD := fmt.Sprintf("kubectl delete pods -n %s -l %s=%s,%s=%s --ignore-not-found=true --grace-period=0 --force", Namespace, dfv1.KeyComponent, dfv1.ComponentISBSvc, dfv1.KeyISBSvcName, ISBSvcName)
	s.Given().When().Exec("sh", []string{"-c", deleteISBPodsCMD}, OutputRegexp(""))
	s.Given().ISBSvc(getISBSvcSpec()).
		When().
		Expect().
		ISBSvcDeleted(defaultTimeout)
	s.T().Log("ISB svc is deleted")
	deleteRedisCMD := fmt.Sprintf("kubectl delete -k ../../config/apps/redis -n %s --ignore-not-found=true", Namespace)
	s.Given().When().Exec("sh", []string{"-c", deleteRedisCMD}, OutputRegexp(`service "redis" deleted`))
	s.T().Log("Redis resources are deleted")
	close(s.stopch)
}

func (s *E2ESuite) CheckError(err error) {
	s.T().Helper()
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *E2ESuite) dynamicFor(r schema.GroupVersionResource) dynamic.ResourceInterface {
	resourceInterface := dynamic.NewForConfigOrDie(s.restConfig).Resource(r)
	return resourceInterface.Namespace(Namespace)
}

func (s *E2ESuite) deleteResources(resources []schema.GroupVersionResource) {
	hasTestLabel := metav1.ListOptions{LabelSelector: Label}
	ctx := context.Background()
	for _, r := range resources {
		err := s.dynamicFor(r).DeleteCollection(ctx, metav1.DeleteOptions{PropagationPolicy: &background}, hasTestLabel)
		s.CheckError(err)
	}

	for _, r := range resources {
		for {
			list, err := s.dynamicFor(r).List(ctx, hasTestLabel)
			s.CheckError(err)
			if len(list.Items) == 0 {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func (s *E2ESuite) Given() *Given {
	return &Given{
		t:                     s.T(),
		isbSvcClient:          s.isbSvcClient,
		pipelineClient:        s.pipelineClient,
		servingPipelineClient: s.servingPipelineClient,
		vertexClient:          s.vertexClient,
		monoVertexClient:      s.monoVertexClient,
		restConfig:            s.restConfig,
		kubeClient:            s.kubeClient,
	}
}

func getISBSvcSpec() string {
	return e2eISBSvcJetStream
}
