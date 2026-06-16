/*
Copyright 2026 The Numaproj Authors.

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestGetISBServiceJetStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newJetStreamMonitorTestHandler(t, map[string]any{
		"10.0.0.1": testJSInfo("server-a", "js-0"),
		"10.0.0.2": testJSInfo("server-b", "js-0"),
	}, nil, true)
	c, w := newISBMonitorTestContext("/api/v1/namespaces/ns/isb-services/default/jetstream")

	h.GetISBServiceJetStream(c)

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Data ISBJetStreamDTO `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Data.Summary, 2)
	assert.Equal(t, "js-0", got.Data.Summary[0].Server)
	assert.Equal(t, "js-1", got.Data.Summary[1].Server)
	assert.Equal(t, "default", got.Data.Summary[0].Cluster)
	assert.Equal(t, 2, got.Data.Summary[0].Streams)
	assert.Equal(t, 3, got.Data.Summary[0].Consumers)
	assert.Equal(t, uint64(10), got.Data.Summary[0].Messages)
	assert.Equal(t, uint64(50), got.Data.Summary[0].Bytes)
	assert.Equal(t, uint64(100), got.Data.Summary[0].APIRequests)
	assert.Equal(t, uint64(5), got.Data.Summary[0].APIErrors)
	assert.Equal(t, 0.05, got.Data.Summary[0].APIErrorRate)
	require.Len(t, got.Data.RaftMetaGroup, 2)
	assert.Equal(t, "js-0", got.Data.RaftMetaGroup[0].Name)
	assert.True(t, got.Data.RaftMetaGroup[0].Leader)
	assert.Equal(t, "js-1", got.Data.RaftMetaGroup[1].Name)
	require.NotNil(t, got.Data.RaftMetaGroup[1].Lag)
	assert.Equal(t, uint64(2), *got.Data.RaftMetaGroup[1].Lag)
	require.Empty(t, got.Data.Errors)
}

func TestGetISBServiceJetStreamPartialFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newJetStreamMonitorTestHandler(t, map[string]any{
		"10.0.0.1": testJSInfo("server-a", "js-0"),
		"10.0.0.2": errors.New("connection refused"),
	}, nil, true)
	c, w := newISBMonitorTestContext("/api/v1/namespaces/ns/isb-services/default/jetstream")

	h.GetISBServiceJetStream(c)

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Data ISBJetStreamDTO `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Data.Summary, 1)
	assert.Equal(t, "js-0", got.Data.Summary[0].Server)
	require.Len(t, got.Data.RaftMetaGroup, 2)
	require.Len(t, got.Data.Errors, 1)
	assert.Equal(t, "js-1", got.Data.Errors[0].Pod)
}

func TestGetISBServiceJetStreamPartialLeaderFailureUsesFollowerRows(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newJetStreamMonitorTestHandler(t, map[string]any{
		"10.0.0.1": errors.New("connection refused"),
		"10.0.0.2": testJSInfoWithoutReplicas("server-b", "js-0"),
		"10.0.0.3": testJSInfoWithoutReplicas("server-c", "js-0"),
	}, []corev1.Pod{
		testISBPod("js-0", "10.0.0.1"),
		testISBPod("js-1", "10.0.0.2"),
		testISBPod("js-2", "10.0.0.3"),
	}, true)
	c, w := newISBMonitorTestContext("/api/v1/namespaces/ns/isb-services/default/jetstream")

	h.GetISBServiceJetStream(c)

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Data ISBJetStreamDTO `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Data.Summary, 2)
	assert.Equal(t, "js-1", got.Data.Summary[0].Server)
	assert.Equal(t, "js-2", got.Data.Summary[1].Server)
	require.Len(t, got.Data.Errors, 1)
	assert.Equal(t, "js-0", got.Data.Errors[0].Pod)
	require.Len(t, got.Data.RaftMetaGroup, 2)
	assert.Equal(t, "js-1", got.Data.RaftMetaGroup[0].Name)
	assert.Equal(t, "server-b", got.Data.RaftMetaGroup[0].ID)
	assert.False(t, got.Data.RaftMetaGroup[0].Leader)
	assert.True(t, got.Data.RaftMetaGroup[0].Online)
	assert.Nil(t, got.Data.RaftMetaGroup[0].Current)
	assert.Nil(t, got.Data.RaftMetaGroup[0].Lag)
	assert.Equal(t, "js-2", got.Data.RaftMetaGroup[1].Name)
	assert.Equal(t, "server-c", got.Data.RaftMetaGroup[1].ID)
}

func TestGetISBServiceJetStreamAllPodsFail(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newJetStreamMonitorTestHandler(t, map[string]any{
		"10.0.0.1": errors.New("connection refused"),
		"10.0.0.2": errors.New("connection refused"),
	}, nil, true)
	c, w := newISBMonitorTestContext("/api/v1/namespaces/ns/isb-services/default/jetstream")

	h.GetISBServiceJetStream(c)

	assert.Equal(t, http.StatusBadGateway, w.Code)
	var got NumaflowAPIResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.NotNil(t, got.ErrMsg)
	assert.Contains(t, *got.ErrMsg, "Failed to fetch JetStream monitor data")
}

func TestGetISBServiceJetStreamNonJetStreamISB(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newJetStreamMonitorTestHandler(t, nil, nil, false)
	c, w := newISBMonitorTestContext("/api/v1/namespaces/ns/isb-services/default/jetstream")

	h.GetISBServiceJetStream(c)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetISBServiceJetStreamNoRunningPods(t *testing.T) {
	gin.SetMode(gin.TestMode)
	pod := testISBPod("js-0", "")
	pod.Status.Phase = corev1.PodPending
	h := newJetStreamMonitorTestHandler(t, nil, []corev1.Pod{pod}, true)
	c, w := newISBMonitorTestContext("/api/v1/namespaces/ns/isb-services/default/jetstream")

	h.GetISBServiceJetStream(c)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetISBServiceJetStreamIPv6PodIP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := newJetStreamMonitorTestHandler(t, map[string]any{
		"fd00::5": testJSInfo("server-a", "js-0"),
	}, []corev1.Pod{
		testISBPod("js-0", "fd00::5"),
	}, true)
	c, w := newISBMonitorTestContext("/api/v1/namespaces/ns/isb-services/default/jetstream")

	h.GetISBServiceJetStream(c)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestDedupeJetStreamRaftMetaDTOsPrefersRicherSameNameRow(t *testing.T) {
	current := true
	lag := uint64(0)
	items := []JetStreamRaftMetaDTO{
		{
			Name:   "js-0",
			ID:     "server-id-from-fallback",
			Online: true,
		},
		{
			Name:    "js-0",
			ID:      "peer-id-from-leader",
			Current: &current,
			Online:  true,
			Active:  "698.81ms",
			Lag:     &lag,
		},
	}

	got := dedupeJetStreamRaftMetaDTOs(items)

	require.Len(t, got, 1)
	assert.Equal(t, "peer-id-from-leader", got[0].ID)
	require.NotNil(t, got[0].Current)
	assert.True(t, *got[0].Current)
	assert.Equal(t, "698.81ms", got[0].Active)
	require.NotNil(t, got[0].Lag)
	assert.Equal(t, uint64(0), *got[0].Lag)
}

func newJetStreamMonitorTestHandler(t *testing.T, responses map[string]any, pods []corev1.Pod, jetStream bool) *handler {
	t.Helper()
	isbsvc := &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "ns"},
		Spec:       dfv1.InterStepBufferServiceSpec{},
	}
	if jetStream {
		isbsvc.Spec.JetStream = &dfv1.JetStreamBufferService{}
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
		numaflowClient: numaflowfake.NewSimpleClientset(isbsvc).NumaflowV1alpha1(),
		httpClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				assert.Equal(t, "/jsz", req.URL.Path)
				assert.Equal(t, "true", req.URL.Query().Get("config"))
				assert.Equal(t, "true", req.URL.Query().Get("accounts"))
				assert.Equal(t, "true", req.URL.Query().Get("streams"))
				assert.Equal(t, "true", req.URL.Query().Get("consumers"))
				host, port, err := net.SplitHostPort(req.URL.Host)
				require.NoError(t, err)
				assert.Equal(t, "8222", port)
				if strings.Contains(host, ":") {
					assert.Equal(t, "["+host+"]:"+port, req.URL.Host)
				}
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

func newISBMonitorTestContext(path string) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, path, nil)
	c.Params = gin.Params{
		{Key: "namespace", Value: "ns"},
		{Key: "isb-service", Value: "default"},
	}
	return c, w
}

func testISBPod(name, ip string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels: map[string]string{
				dfv1.KeyComponent:  dfv1.ComponentISBSvc,
				dfv1.KeyISBSvcName: "default",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: ip,
		},
	}
}

func testJSInfo(serverID, leader string) natsserver.JSInfo {
	return natsserver.JSInfo{
		JetStreamStats: natsserver.JetStreamStats{
			API: natsserver.JetStreamAPIStats{
				Total:  100,
				Errors: 5,
			},
		},
		ID:        serverID,
		Streams:   2,
		Consumers: 3,
		Messages:  10,
		Bytes:     50,
		Meta: &natsserver.MetaClusterInfo{
			Name:   "default",
			Leader: leader,
			Peer:   serverID,
			Replicas: []*natsserver.PeerInfo{
				{Name: "js-1", Peer: "server-b", Current: true, Active: time.Second, Lag: 2},
			},
		},
	}
}

func testJSInfoWithoutReplicas(serverID, leader string) natsserver.JSInfo {
	jsInfo := testJSInfo(serverID, leader)
	jsInfo.Meta.Replicas = nil
	return jsInfo
}
