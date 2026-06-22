package v1

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	natsserver "github.com/nats-io/nats-server/v2/server"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	numaflowfake "github.com/numaproj/numaflow/pkg/client/clientset/versioned/fake"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func TestGetPipelineISBStreamsVertexScoped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newPipelineISBDebugTestHandler(t, map[string]any{
		"10.0.0.1": testPipelineISBJSInfo(),
	}, []corev1.Pod{testISBPod("js-0", "10.0.0.1")}, true)
	c, w := newPipelineISBDebugTestContext("/api/v1/namespaces/ns/pipelines/pl/isb/streams?vertex=cat")

	h.GetPipelineISBStreams(c)

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Data PipelineISBStreamsDTO `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Data.Streams, 2)
	assert.Equal(t, "cat", got.Data.Streams[0].Vertex)
	assert.Equal(t, 0, got.Data.Streams[0].Partition)
	assert.Equal(t, dfv1.GenerateBufferName("ns", "pl", "cat", 0), got.Data.Streams[0].Stream)
	assert.Equal(t, []string{dfv1.GenerateBufferName("ns", "pl", "cat", 0)}, got.Data.Streams[0].Subjects)
	assert.Equal(t, uint64(10), got.Data.Streams[0].Messages)
	assert.Equal(t, "file", got.Data.Streams[0].Storage)
	assert.Equal(t, "workqueue", got.Data.Streams[0].Retention)
	assert.Equal(t, "js-0", got.Data.Streams[0].Leader)
	require.Empty(t, got.Data.Errors)
}

func TestGetPipelineISBConsumersEdgeScoped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newPipelineISBDebugTestHandler(t, map[string]any{
		"10.0.0.1": testPipelineISBJSInfo(),
	}, []corev1.Pod{testISBPod("js-0", "10.0.0.1")}, true)
	c, w := newPipelineISBDebugTestContext("/api/v1/namespaces/ns/pipelines/pl/isb/consumers?from=in&to=cat&partition=1")

	h.GetPipelineISBConsumers(c)

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Data PipelineISBConsumersDTO `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Data.Consumers, 1)
	consumer := got.Data.Consumers[0]
	assert.Equal(t, "cat", consumer.Vertex)
	assert.Equal(t, "in", consumer.From)
	assert.Equal(t, "cat", consumer.To)
	assert.Equal(t, 1, consumer.Partition)
	assert.Equal(t, "targetVertex", consumer.Scope)
	assert.Equal(t, dfv1.GenerateBufferName("ns", "pl", "cat", 1), consumer.Stream)
	assert.Equal(t, dfv1.GenerateBufferName("ns", "pl", "cat", 1), consumer.Consumer)
	assert.Equal(t, "explicit", consumer.AckPolicy)
	assert.Equal(t, float64(30), consumer.AckWaitSeconds)
	assert.Equal(t, 7, consumer.NumAckPending)
	assert.Equal(t, uint64(21), consumer.DeliveredStreamSeq)
	assert.Equal(t, uint64(14), consumer.AckFloorStreamSeq)
}

func TestGetPipelineISBConsumersIncludesDirectConsumers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jsInfo := testPipelineISBJSInfo()
	cat1 := dfv1.GenerateBufferName("ns", "pl", "cat", 1)
	jsInfo.AccountDetails[0].Streams[1].DirectConsumer = []*natsserver.ConsumerInfo{
		{
			Stream: cat1,
			Name:   "direct-cat-1",
			Config: &natsserver.ConsumerConfig{
				Name:      "direct-cat-1",
				AckPolicy: natsserver.AckExplicit,
			},
			NumPending: 5,
		},
	}
	h := newPipelineISBDebugTestHandler(t, map[string]any{
		"10.0.0.1": jsInfo,
	}, []corev1.Pod{testISBPod("js-0", "10.0.0.1")}, true)
	c, w := newPipelineISBDebugTestContext("/api/v1/namespaces/ns/pipelines/pl/isb/consumers?from=in&to=cat&partition=1")

	h.GetPipelineISBConsumers(c)

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Data PipelineISBConsumersDTO `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Data.Consumers, 2)
	var directConsumer *PipelineISBConsumerDTO
	for i := range got.Data.Consumers {
		if got.Data.Consumers[i].Consumer == "direct-cat-1" {
			directConsumer = &got.Data.Consumers[i]
			break
		}
	}
	require.NotNil(t, directConsumer)
	assert.Equal(t, cat1, directConsumer.Stream)
	assert.Equal(t, "explicit", directConsumer.AckPolicy)
	assert.Equal(t, uint64(5), directConsumer.NumPending)
}

func TestGetPipelineISBKVStoresVertexScoped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newPipelineISBDebugTestHandler(t, map[string]any{
		"10.0.0.1": testPipelineISBJSInfo(),
	}, []corev1.Pod{testISBPod("js-0", "10.0.0.1")}, true)
	c, w := newPipelineISBDebugTestContext("/api/v1/namespaces/ns/pipelines/pl/isb/kv-stores?vertex=cat")

	h.GetPipelineISBKVStores(c)

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Data PipelineISBKVStoresDTO `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Data.KVStores, 2)
	readBucket := isbsvc.JetStreamOTKVName(dfv1.GenerateEdgeBucketName("ns", "pl", "in", "cat"))
	writeBucket := isbsvc.JetStreamOTKVName(dfv1.GenerateEdgeBucketName("ns", "pl", "cat", "out"))
	assert.Equal(t, readBucket, got.Data.KVStores[0].Bucket)
	assert.Equal(t, "read", got.Data.KVStores[0].Direction)
	assert.Equal(t, "KV_"+readBucket, got.Data.KVStores[0].Stream)
	assert.Equal(t, writeBucket, got.Data.KVStores[1].Bucket)
	assert.Equal(t, "write", got.Data.KVStores[1].Direction)
	assert.Equal(t, 2, got.Data.KVStores[0].Values)
	assert.Equal(t, uint64(30), got.Data.KVStores[0].Bytes)
	assert.Equal(t, int64(1), got.Data.KVStores[0].History)
	assert.Equal(t, float64(3600), got.Data.KVStores[0].TTLSeconds)
}

func TestGetPipelineISBStreamsPartialFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newPipelineISBDebugTestHandler(t, map[string]any{
		"10.0.0.1": testPipelineISBJSInfo(),
		"10.0.0.2": errors.New("connection refused"),
	}, nil, true)
	c, w := newPipelineISBDebugTestContext("/api/v1/namespaces/ns/pipelines/pl/isb/streams?vertex=cat")

	h.GetPipelineISBStreams(c)

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Data PipelineISBStreamsDTO `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Data.Streams, 2)
	require.Len(t, got.Data.Errors, 1)
	assert.Equal(t, "js-1", got.Data.Errors[0].Pod)
}

func TestGetPipelineISBStreamsAllPodsFail(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newPipelineISBDebugTestHandler(t, map[string]any{
		"10.0.0.1": errors.New("connection refused"),
		"10.0.0.2": errors.New("connection refused"),
	}, nil, true)
	c, w := newPipelineISBDebugTestContext("/api/v1/namespaces/ns/pipelines/pl/isb/streams?vertex=cat")

	h.GetPipelineISBStreams(c)

	assert.Equal(t, http.StatusBadGateway, w.Code)
}

func TestGetPipelineISBDebugInvalidScope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		path string
	}{
		{name: "conflicting filters", path: "/api/v1/namespaces/ns/pipelines/pl/isb/streams?vertex=cat&from=in&to=cat"},
		{name: "missing to", path: "/api/v1/namespaces/ns/pipelines/pl/isb/streams?from=in"},
		{name: "unknown vertex", path: "/api/v1/namespaces/ns/pipelines/pl/isb/streams?vertex=missing"},
		{name: "unknown edge", path: "/api/v1/namespaces/ns/pipelines/pl/isb/streams?from=in&to=out"},
		{name: "bad partition", path: "/api/v1/namespaces/ns/pipelines/pl/isb/streams?vertex=cat&partition=bad"},
		{name: "out of range partition", path: "/api/v1/namespaces/ns/pipelines/pl/isb/streams?vertex=cat&partition=4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newPipelineISBDebugTestHandler(t, map[string]any{"10.0.0.1": testPipelineISBJSInfo()}, []corev1.Pod{testISBPod("js-0", "10.0.0.1")}, true)
			c, w := newPipelineISBDebugTestContext(tt.path)

			h.GetPipelineISBStreams(c)

			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestGetPipelineISBDebugNonJetStreamISB(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newPipelineISBDebugTestHandler(t, nil, nil, false)
	c, w := newPipelineISBDebugTestContext("/api/v1/namespaces/ns/pipelines/pl/isb/streams?vertex=cat")

	h.GetPipelineISBStreams(c)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func newPipelineISBDebugTestHandler(t *testing.T, responses map[string]any, pods []corev1.Pod, jetStream bool) *handler {
	t.Helper()
	isbsvcObj := &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "ns"},
		Spec:       dfv1.InterStepBufferServiceSpec{},
	}
	if jetStream {
		isbsvcObj.Spec.JetStream = &dfv1.JetStreamBufferService{}
	}
	if pods == nil {
		pods = []corev1.Pod{
			testISBPod("js-0", "10.0.0.1"),
			testISBPod("js-1", "10.0.0.2"),
		}
	}
	kubeObjects := make([]runtime.Object, 0, len(pods))
	for i := range pods {
		kubeObjects = append(kubeObjects, &pods[i])
	}
	return &handler{
		kubeClient:     kubefake.NewSimpleClientset(kubeObjects...),
		numaflowClient: numaflowfake.NewSimpleClientset(isbsvcObj, testPipelineISBDebugPipeline()).NumaflowV1alpha1(),
		httpClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				assert.Equal(t, "/jsz", req.URL.Path)
				host, _, err := net.SplitHostPort(req.URL.Host)
				require.NoError(t, err)
				value, ok := responses[host]
				if !ok {
					return nil, errors.New("unexpected host")
				}
				if err, ok := value.(error); ok {
					return nil, err
				}
				body, err := json.Marshal(value)
				require.NoError(t, err)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(string(body))),
					Header:     make(http.Header),
				}, nil
			}),
		},
	}
}

func newPipelineISBDebugTestContext(path string) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, path, nil)
	c.Params = gin.Params{
		{Key: "namespace", Value: "ns"},
		{Key: "pipeline", Value: "pl"},
	}
	return c, w
}

func testPipelineISBDebugPipeline() *dfv1.Pipeline {
	partitions := int32(2)
	return &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "pl", Namespace: "ns"},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{Name: "in", Source: &dfv1.Source{}},
				{Name: "cat", UDF: &dfv1.UDF{}, Partitions: &partitions},
				{Name: "out", Sink: &dfv1.Sink{}},
			},
			Edges: []dfv1.Edge{
				{From: "in", To: "cat"},
				{From: "cat", To: "out"},
			},
		},
	}
}

func testPipelineISBJSInfo() natsserver.JSInfo {
	cat0 := dfv1.GenerateBufferName("ns", "pl", "cat", 0)
	cat1 := dfv1.GenerateBufferName("ns", "pl", "cat", 1)
	out0 := dfv1.GenerateBufferName("ns", "pl", "out", 0)
	inCatBucket := isbsvc.JetStreamOTKVName(dfv1.GenerateEdgeBucketName("ns", "pl", "in", "cat"))
	catOutBucket := isbsvc.JetStreamOTKVName(dfv1.GenerateEdgeBucketName("ns", "pl", "cat", "out"))
	return natsserver.JSInfo{
		ID: "server-a",
		AccountDetails: []*natsserver.AccountDetail{
			{
				Name: "$G",
				Streams: []natsserver.StreamDetail{
					testPipelineISBStreamDetail(cat0, 10, 100, 0),
					testPipelineISBStreamDetail(cat1, 11, 110, 1),
					testPipelineISBStreamDetail(out0, 12, 120, 0),
					testPipelineISBKVStreamDetail(inCatBucket),
					testPipelineISBKVStreamDetail(catOutBucket),
				},
			},
		},
	}
}

func testPipelineISBStreamDetail(name string, messages uint64, bytes uint64, partition int) natsserver.StreamDetail {
	return natsserver.StreamDetail{
		Name: name,
		Cluster: &natsserver.ClusterInfo{
			Leader: "js-0",
		},
		Config: &natsserver.StreamConfig{
			Name:      name,
			Subjects:  []string{name},
			Retention: natsserver.WorkQueuePolicy,
			Storage:   natsserver.FileStorage,
			Replicas:  3,
		},
		State: natsserver.StreamState{
			Msgs:      messages,
			Bytes:     bytes,
			FirstSeq:  1,
			LastSeq:   messages,
			Consumers: 1,
		},
		Consumer: []*natsserver.ConsumerInfo{
			{
				Stream: name,
				Name:   name,
				Config: &natsserver.ConsumerConfig{
					Durable:       name,
					Name:          name,
					FilterSubject: name,
					AckPolicy:     natsserver.AckExplicit,
					DeliverPolicy: natsserver.DeliverAll,
					AckWait:       30 * time.Second,
					MaxAckPending: 25000,
				},
				Delivered: natsserver.SequenceInfo{
					Consumer: uint64(20 + partition),
					Stream:   uint64(20 + partition),
				},
				AckFloor: natsserver.SequenceInfo{
					Consumer: uint64(13 + partition),
					Stream:   uint64(13 + partition),
				},
				NumAckPending:  7,
				NumRedelivered: 1,
				NumWaiting:     2,
				NumPending:     3,
				Cluster: &natsserver.ClusterInfo{
					Leader: "js-0",
				},
			},
		},
	}
}

func testPipelineISBKVStreamDetail(bucket string) natsserver.StreamDetail {
	stream := "KV_" + bucket
	return natsserver.StreamDetail{
		Name: stream,
		Config: &natsserver.StreamConfig{
			Name:       stream,
			MaxMsgsPer: 1,
			MaxAge:     time.Hour,
			Storage:    natsserver.FileStorage,
			Replicas:   3,
		},
		State: natsserver.StreamState{
			Msgs:        3,
			Bytes:       30,
			NumSubjects: 2,
		},
	}
}
