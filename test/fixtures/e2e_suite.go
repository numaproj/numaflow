package fixtures

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeutil "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	flowversiond "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	flowpkg "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
)

const (
	Namespace      = "numaflow-system"
	Label          = "numaflow-e2e"
	LabelValue     = "true"
	ISBSvcName     = "numaflow-e2e"
	defaultTimeout = 60 * time.Second

	LogSourceVertexStarted = "Start processing source messages"
	LogSinkVertexStarted   = "Start processing sink messages"
	LogUDFVertexStarted    = "Start processing udf messages"
)

var (
	background = metav1.DeletePropagationBackground

	e2eISBSvcRedis = `apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  redis:
    native:
      version: 6.2.6`

	e2eISBSvcJetStream = `apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  jetstream:
    version: latest`
)

type E2ESuite struct {
	suite.Suite
	restConfig     *rest.Config
	isbSvcClient   flowpkg.InterStepBufferServiceInterface
	pipelineClient flowpkg.PipelineInterface
	vertexClient   flowpkg.VertexInterface
	kubeClient     kubernetes.Interface
	stopch         chan struct{}
}

func (s *E2ESuite) SetupSuite() {
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
		s.restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		s.restConfig, err = rest.InClusterConfig()
	}
	s.stopch = make(chan struct{})
	s.CheckError(err)
	s.kubeClient, err = kubernetes.NewForConfig(s.restConfig)
	s.CheckError(err)
	s.isbSvcClient = flowversiond.NewForConfigOrDie(s.restConfig).NumaflowV1alpha1().InterStepBufferServices(Namespace)
	s.pipelineClient = flowversiond.NewForConfigOrDie(s.restConfig).NumaflowV1alpha1().Pipelines(Namespace)
	s.vertexClient = flowversiond.NewForConfigOrDie(s.restConfig).NumaflowV1alpha1().Vertices(Namespace)

	// Clean up resources if any
	s.deleteResources([]schema.GroupVersionResource{
		dfv1.PipelineGroupVersionResource,
		dfv1.ISBGroupVersionResource,
		batchv1.SchemeGroupVersion.WithResource("jobs"),
	})
	s.Given().ISBSvc(getISBSvcSpec()).
		When().
		Expect().
		ISBSvcDeleted(1 * time.Minute)

	s.Given().ISBSvc(getISBSvcSpec()).
		When().
		CreateISBSvc().
		WaitForISBSvcReady()
	s.T().Log("ISB svc is ready")
	err = PodPortForward(s.restConfig, Namespace, "e2e-api-pod", 8378, 8378, s.stopch)
	s.CheckError(err)
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
		Wait(3 * time.Second).
		Expect().
		ISBSvcDeleted(1 * time.Minute)
	s.T().Log("ISB svc is deleted")
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
		t:              s.T(),
		isbSvcClient:   s.isbSvcClient,
		pipelineClient: s.pipelineClient,
		vertexClient:   s.vertexClient,
		restConfig:     s.restConfig,
		kubeClient:     s.kubeClient,
	}
}

func (s *E2ESuite) StartPortForward(podName string, port int) (stopPortForward func()) {

	s.T().Log("starting port-forward to pod :", podName, port)
	transport, upgrader, err := spdy.RoundTripperFor(s.restConfig)
	if err != nil {
		panic(err)
	}
	x, err := url.Parse(fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s/portforward", s.restConfig.Host, Namespace, podName))
	if err != nil {
		panic(err)
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", x)
	stopChan, readyChan := make(chan struct{}, 1), make(chan struct{}, 1)
	forwarder, err := portforward.New(dialer, []string{fmt.Sprintf("%d:%d", port, port)}, stopChan, readyChan, os.Stdout, os.Stderr)
	if err != nil {
		panic(err)
	}
	go func() {
		defer runtimeutil.HandleCrash()
		if err := forwarder.ForwardPorts(); err != nil {
			panic(err)
		}
	}()
	<-readyChan
	s.T().Log("started port-forward :", podName, port)
	return func() {
		stopChan <- struct{}{}
		// not needed
		// forwarder.Close()
		s.T().Log("stopped port-forward :", podName, port)
	}
}

func getISBSvcSpec() string {
	x := strings.ToUpper(os.Getenv("ISBSVC"))
	if x == "REDIS" {
		return e2eISBSvcRedis
	}
	return e2eISBSvcJetStream
}
