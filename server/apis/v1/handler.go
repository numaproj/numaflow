package v1

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsversiond "k8s.io/metrics/pkg/client/clientset/versioned"
	"k8s.io/utils/pointer"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	dfv1versiond "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
)

type handler struct {
	kubeClient     kubernetes.Interface
	metricsClient  *metricsversiond.Clientset
	numaflowClient dfv1clients.NumaflowV1alpha1Interface
}

func NewHandler() (*handler, error) {
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
		return nil, fmt.Errorf("failed to get kubeconfig, %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeclient, %w", err)
	}
	metricsClient := metricsversiond.NewForConfigOrDie(restConfig)
	numaflowClient := dfv1versiond.NewForConfigOrDie(restConfig).NumaflowV1alpha1()
	return &handler{
		kubeClient:     kubeClient,
		metricsClient:  metricsClient,
		numaflowClient: numaflowClient,
	}, nil
}

func (h *handler) ListPipelines(c *gin.Context) {
	plList, err := h.numaflowClient.Pipelines(c.Param("namespace")).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, plList.Items)
}

func (h *handler) GetPipeline(c *gin.Context) {
	pl, err := h.numaflowClient.Pipelines(c.Param("namespace")).Get(context.Background(), c.Param("pipeline"), metav1.GetOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, pl)
}

func (h *handler) ListNamespaces(c *gin.Context) {
	l, err := h.numaflowClient.Pipelines("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	m := make(map[string]bool)
	for _, pl := range l.Items {
		m[pl.Namespace] = true
	}
	namespaces := []string{}
	for k := range m {
		namespaces = append(namespaces, k)
	}
	c.JSON(http.StatusOK, namespaces)
}

func (h *handler) ListInterStepBufferServices(c *gin.Context) {
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	isbSvcs, err := h.numaflowClient.InterStepBufferServices(c.Param("namespace")).List(context.Background(), metav1.ListOptions{
		Limit:    limit,
		Continue: c.Query("continue"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, isbSvcs.Items)
}

func (h *handler) GetInterStepBufferService(c *gin.Context) {
	isbsvc, err := h.numaflowClient.InterStepBufferServices(c.Param("namespace")).Get(context.Background(), c.Param("isbsvc"), metav1.GetOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, isbsvc)
}

func (h *handler) ListVerices(c *gin.Context) {
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	vertices, err := h.numaflowClient.Vertices(c.Param("namespace")).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", dfv1.KeyPipelineName, c.Param("pipeline")),
		Limit:         limit,
		Continue:      c.Query("continue"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, vertices.Items)
}

func (h *handler) GetVertex(c *gin.Context) {
	vertices, err := h.numaflowClient.Vertices(c.Param("namespace")).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, c.Param("pipeline"), dfv1.KeyVertexName, c.Param("vertex")),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	if len(vertices.Items) == 0 {
		c.JSON(http.StatusNotFound, fmt.Sprintf("Vertex %q not found", c.Param("vertex")))
		return
	}
	c.JSON(http.StatusOK, vertices.Items[0])
}

func (h *handler) ListVertexPods(c *gin.Context) {
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	pods, err := h.kubeClient.CoreV1().Pods(c.Param("namespace")).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, c.Param("pipeline"), dfv1.KeyVertexName, c.Param("vertex")),
		Limit:         limit,
		Continue:      c.Query("continue"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, pods.Items)
}

func (h *handler) ListPodsMetrics(c *gin.Context) {
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	l, err := h.metricsClient.MetricsV1beta1().PodMetricses(c.Param("namespace")).List(context.Background(), metav1.ListOptions{
		Limit:    limit,
		Continue: c.Query("continue"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, l.Items)
}

func (h *handler) GetPodMetrics(c *gin.Context) {
	m, err := h.metricsClient.MetricsV1beta1().PodMetricses(c.Param("namespace")).Get(context.Background(), c.Param("pod"), metav1.GetOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, m)
}

func (h *handler) PodLogs(c *gin.Context) {
	var tailLines *int64
	if v := c.Query("tailLines"); v != "" {
		x, _ := strconv.ParseInt(v, 10, 64)
		tailLines = pointer.Int64(x)
	}
	stream, err := h.kubeClient.CoreV1().
		Pods(c.Param("namespace")).
		GetLogs(c.Param("pod"), &corev1.PodLogOptions{
			Container: c.Query("container"),
			Follow:    c.Query("follow") == "true",
			TailLines: tailLines,
		}).Stream(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	defer stream.Close()
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		_, _ = c.Writer.Write(scanner.Bytes())
		_, _ = c.Writer.WriteString("\n")
		c.Writer.Flush()
	}
}

func (h *handler) ListPipelineEdges(c *gin.Context) {
	ns := c.Param("namespace")
	pipeline := c.Param("pipeline")
	client, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeline))
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	l, err := client.ListPipelineBuffers(context.Background(), pipeline)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, l)
}

func (h *handler) GetPipelineEdge(c *gin.Context) {
	ns := c.Param("namespace")
	pipeline := c.Param("pipeline")
	client, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeline))
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	// Assume edge is the buffer name
	i, err := client.GetPipelineBuffer(context.Background(), pipeline, c.Param("edge"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, i)
}

func daemonSvcAddress(ns, pipeline string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", fmt.Sprintf("%s-daemon-svc", pipeline), ns, dfv1.DaemonServicePort)
}
