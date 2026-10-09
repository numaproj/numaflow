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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	fakeClient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func TestPodMatchesLogScope(t *testing.T) {
	pipelinePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		dfv1.KeyPartOf:       dfv1.Project,
		dfv1.KeyManagedBy:    dfv1.ControllerVertex,
		dfv1.KeyComponent:    dfv1.ComponentVertex,
		dfv1.KeyPipelineName: "my-pipeline",
		dfv1.KeyVertexName:   "my-vertex",
	}}}
	monoVertexPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		dfv1.KeyPartOf:         dfv1.Project,
		dfv1.KeyManagedBy:      dfv1.ControllerMonoVertex,
		dfv1.KeyComponent:      dfv1.ComponentMonoVertex,
		dfv1.KeyMonoVertexName: "my-mono-vertex",
	}}}
	daemonPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		dfv1.KeyPartOf:       dfv1.Project,
		dfv1.KeyManagedBy:    dfv1.ControllerPipeline,
		dfv1.KeyComponent:    dfv1.ComponentDaemon,
		dfv1.KeyPipelineName: "my-pipeline",
	}}}
	sideInputManagerPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		dfv1.KeyPartOf:        dfv1.Project,
		dfv1.KeyManagedBy:     dfv1.ControllerPipeline,
		dfv1.KeyComponent:     dfv1.ComponentSideInputManager,
		dfv1.KeyPipelineName:  "my-pipeline",
		dfv1.KeySideInputName: "my-side-input",
	}}}
	isbServicePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		dfv1.KeyPartOf:     dfv1.Project,
		dfv1.KeyManagedBy:  dfv1.ControllerISBSvc,
		dfv1.KeyComponent:  dfv1.ComponentISBSvc,
		dfv1.KeyISBSvcName: "my-isb-service",
	}}}
	servingPipelinePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		dfv1.KeyPartOf:              dfv1.Project,
		dfv1.KeyManagedBy:           dfv1.ControllerServingPipeline,
		dfv1.KeyComponent:           dfv1.ComponentServingServer,
		dfv1.KeyServingPipelineName: "my-serving-pipeline",
	}}}
	unrelatedPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		"app": "unrelated",
	}}}

	tests := []struct {
		name           string
		pod            *corev1.Pod
		requiredLabels map[string]string
		expected       bool
	}{
		{
			name: "pipeline pod matches its pipeline and vertex",
			pod:  pipelinePod,
			requiredLabels: map[string]string{
				dfv1.KeyPipelineName: "my-pipeline",
				dfv1.KeyVertexName:   "my-vertex",
			},
			expected: true,
		},
		{
			name: "pipeline pod does not match another pipeline",
			pod:  pipelinePod,
			requiredLabels: map[string]string{
				dfv1.KeyPipelineName: "other-pipeline",
				dfv1.KeyVertexName:   "my-vertex",
			},
			expected: false,
		},
		{
			name: "pipeline pod does not match another vertex",
			pod:  pipelinePod,
			requiredLabels: map[string]string{
				dfv1.KeyPipelineName: "my-pipeline",
				dfv1.KeyVertexName:   "other-vertex",
			},
			expected: false,
		},
		{
			name: "mono vertex pod matches its mono vertex",
			pod:  monoVertexPod,
			requiredLabels: map[string]string{
				dfv1.KeyMonoVertexName: "my-mono-vertex",
			},
			expected: true,
		},
		{
			name: "mono vertex pod does not match another mono vertex",
			pod:  monoVertexPod,
			requiredLabels: map[string]string{
				dfv1.KeyMonoVertexName: "other-mono-vertex",
			},
			expected: false,
		},
		{name: "legacy route accepts pipeline pod", pod: pipelinePod, expected: true},
		{name: "legacy route accepts mono vertex pod", pod: monoVertexPod, expected: true},
		{name: "legacy route accepts pipeline daemon pod", pod: daemonPod, expected: true},
		{name: "legacy route accepts side input manager pod", pod: sideInputManagerPod, expected: true},
		{name: "legacy route accepts ISB service pod", pod: isbServicePod, expected: true},
		{name: "legacy route accepts serving pipeline pod", pod: servingPipelinePod, expected: true},
		{name: "legacy route rejects unrelated pod", pod: unrelatedPod, expected: false},
		{
			name: "legacy route rejects an unknown manager",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				dfv1.KeyPartOf:       dfv1.Project,
				dfv1.KeyManagedBy:    "unrelated-controller",
				dfv1.KeyPipelineName: "my-pipeline",
			}}},
			expected: false,
		},
		{
			name: "legacy route rejects spoofed resource labels without Numaflow ownership",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				dfv1.KeyPipelineName: "my-pipeline",
				dfv1.KeyVertexName:   "my-vertex",
			}}},
			expected: false,
		},
		{
			name: "legacy route rejects partial pipeline labels",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				dfv1.KeyPartOf:       dfv1.Project,
				dfv1.KeyManagedBy:    dfv1.ControllerVertex,
				dfv1.KeyPipelineName: "my-pipeline",
			}}},
			expected: false,
		},
		{name: "legacy route rejects unlabeled pod", pod: &corev1.Pod{}, expected: false},
		{
			name: "scoped route rejects empty resource name",
			pod:  pipelinePod,
			requiredLabels: map[string]string{
				dfv1.KeyPipelineName: "",
				dfv1.KeyVertexName:   "my-vertex",
			},
			expected: false,
		},
		{
			name: "scoped route rejects matching labels without Numaflow ownership",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				dfv1.KeyPipelineName: "my-pipeline",
				dfv1.KeyVertexName:   "my-vertex",
			}}},
			requiredLabels: map[string]string{
				dfv1.KeyPipelineName: "my-pipeline",
				dfv1.KeyVertexName:   "my-vertex",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, podMatchesLogScope(tt.pod, tt.requiredLabels))
		})
	}
}

func TestScopedPodLogsRejectUnrelatedPodBeforeStreaming(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const (
		namespace = "shared-namespace"
		podName   = "unrelated-payment-api"
	)

	tests := []struct {
		name       string
		route      string
		requestURL string
		handle     func(*handler, *gin.Context)
	}{
		{
			name:       "pipeline route",
			route:      "/api/v1/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods/:pod/logs",
			requestURL: "/api/v1/namespaces/shared-namespace/pipelines/my-pipeline/vertices/my-vertex/pods/unrelated-payment-api/logs?container=app",
			handle:     (*handler).PipelinePodLogs,
		},
		{
			name:       "mono vertex route",
			route:      "/api/v1/namespaces/:namespace/mono-vertices/:mono-vertex/pods/:pod/logs",
			requestURL: "/api/v1/namespaces/shared-namespace/mono-vertices/my-mono-vertex/pods/unrelated-payment-api/logs?container=app",
			handle:     (*handler).MonoVertexPodLogs,
		},
		{
			name:       "legacy route",
			route:      "/api/v1/namespaces/:namespace/pods/:pod/logs",
			requestURL: "/api/v1/namespaces/shared-namespace/pods/unrelated-payment-api/logs?container=app",
			handle:     (*handler).PodLogs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kubeClient := fakeClient.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: namespace,
				Labels:    map[string]string{"app": "unrelated-payment-api"},
			}})
			h := &handler{kubeClient: kubeClient}
			router := gin.New()
			router.GET(tt.route, func(c *gin.Context) { tt.handle(h, c) })

			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tt.requestURL, nil))

			assert.Equal(t, http.StatusForbidden, response.Code)
			var body NumaflowAPIResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.NotNil(t, body.ErrMsg)
			assert.Equal(t, "Pod does not belong to the requested Numaflow resource", *body.ErrMsg)
			require.Len(t, kubeClient.Actions(), 1)
			assert.Equal(t, "get", kubeClient.Actions()[0].GetVerb())
			assert.Equal(t, "pods", kubeClient.Actions()[0].GetResource().Resource)
		})
	}
}

func TestScopedPodLogsStreamsMatchingPod(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name       string
		route      string
		requestURL string
		labels     map[string]string
		handle     func(*handler, *gin.Context)
	}{
		{
			name:       "pipeline pod",
			route:      "/api/v1/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods/:pod/logs",
			requestURL: "/api/v1/namespaces/test-ns/pipelines/my-pipeline/vertices/my-vertex/pods/test-pod/logs?container=main&follow=true&tailLines=25",
			labels: map[string]string{
				dfv1.KeyPartOf:       dfv1.Project,
				dfv1.KeyManagedBy:    dfv1.ControllerVertex,
				dfv1.KeyComponent:    dfv1.ComponentVertex,
				dfv1.KeyPipelineName: "my-pipeline",
				dfv1.KeyVertexName:   "my-vertex",
			},
			handle: (*handler).PipelinePodLogs,
		},
		{
			name:       "mono vertex pod",
			route:      "/api/v1/namespaces/:namespace/mono-vertices/:mono-vertex/pods/:pod/logs",
			requestURL: "/api/v1/namespaces/test-ns/mono-vertices/my-mono-vertex/pods/test-pod/logs?container=main&follow=true&tailLines=25",
			labels: map[string]string{
				dfv1.KeyPartOf:         dfv1.Project,
				dfv1.KeyManagedBy:      dfv1.ControllerMonoVertex,
				dfv1.KeyComponent:      dfv1.ComponentMonoVertex,
				dfv1.KeyMonoVertexName: "my-mono-vertex",
			},
			handle: (*handler).MonoVertexPodLogs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logRequests := 0
			kubeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v1/namespaces/test-ns/pods/test-pod":
					w.Header().Set("Content-Type", "application/json")
					if err := json.NewEncoder(w).Encode(corev1.Pod{
						TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-pod",
							Namespace: "test-ns",
							Labels:    tt.labels,
						},
					}); err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
					}
				case "/api/v1/namespaces/test-ns/pods/test-pod/log":
					logRequests++
					assert.Equal(t, "main", r.URL.Query().Get("container"))
					assert.Equal(t, "true", r.URL.Query().Get("follow"))
					assert.Equal(t, "25", r.URL.Query().Get("tailLines"))
					assert.Equal(t, "true", r.URL.Query().Get("timestamps"))
					_, _ = fmt.Fprintln(w, "matching pod log")
				default:
					http.NotFound(w, r)
				}
			}))
			defer kubeAPI.Close()

			kubeClient, err := kubernetes.NewForConfig(&rest.Config{Host: kubeAPI.URL})
			require.NoError(t, err)
			h := &handler{kubeClient: kubeClient}
			router := gin.New()
			router.GET(tt.route, func(c *gin.Context) { tt.handle(h, c) })

			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tt.requestURL, nil))

			assert.Equal(t, http.StatusOK, response.Code)
			assert.Equal(t, "matching pod log\n", response.Body.String())
			assert.Equal(t, 1, logRequests)
		})
	}
}

func TestScopedPodLogsReturnsKubernetesErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const route = "/api/v1/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods/:pod/logs"
	const requestURL = "/api/v1/namespaces/test-ns/pipelines/my-pipeline/vertices/my-vertex/pods/test-pod/logs"

	t.Run("pod lookup fails", func(t *testing.T) {
		h := &handler{kubeClient: fakeClient.NewSimpleClientset()}
		router := gin.New()
		router.GET(route, h.PipelinePodLogs)

		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestURL, nil))

		assert.Equal(t, http.StatusOK, response.Code)
		var body NumaflowAPIResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.NotNil(t, body.ErrMsg)
		assert.Contains(t, *body.ErrMsg, "Failed to get pod \"test-pod\" in namespace \"test-ns\"")
	})

	t.Run("log stream fails", func(t *testing.T) {
		kubeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/v1/namespaces/test-ns/pods/test-pod":
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(corev1.Pod{
					TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod",
						Namespace: "test-ns",
						Labels: map[string]string{
							dfv1.KeyPartOf:       dfv1.Project,
							dfv1.KeyManagedBy:    dfv1.ControllerVertex,
							dfv1.KeyComponent:    dfv1.ComponentVertex,
							dfv1.KeyPipelineName: "my-pipeline",
							dfv1.KeyVertexName:   "my-vertex",
						},
					},
				}); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
			case "/api/v1/namespaces/test-ns/pods/test-pod/log":
				http.Error(w, "log backend unavailable", http.StatusInternalServerError)
			default:
				http.NotFound(w, r)
			}
		}))
		defer kubeAPI.Close()

		kubeClient, err := kubernetes.NewForConfig(&rest.Config{Host: kubeAPI.URL})
		require.NoError(t, err)
		h := &handler{kubeClient: kubeClient}
		router := gin.New()
		router.GET(route, h.PipelinePodLogs)

		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestURL, nil))

		assert.Equal(t, http.StatusOK, response.Code)
		var body NumaflowAPIResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.NotNil(t, body.ErrMsg)
		assert.Contains(t, *body.ErrMsg, "Failed to get pod logs")
	})
}
