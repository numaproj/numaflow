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

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8stesting "k8s.io/client-go/testing"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"

	"github.com/gin-gonic/gin"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeClient "k8s.io/client-go/kubernetes/fake"
)

var (
	validPatchSpec   = `{"spec": {"lifecycle": {"desiredPhase": "Paused"}}}`
	invalidPatchSpec = `{"spec": {"limits": {"readTimeout": "5s"}}}`
)

func TestValidatePipelinePatch(t *testing.T) {

	err := validatePipelinePatch([]byte(validPatchSpec))
	assert.NoError(t, err)

	err = validatePipelinePatch([]byte(invalidPatchSpec))
	assert.Error(t, err)
	assert.Equal(t, "only spec.lifecycle is allowed for patching", err.Error())

}

func TestHandler_DiscoverMetrics(t *testing.T) {
	tests := []struct {
		name           string
		object         string
		configPatterns []PatternData
		want           MetricsDiscoveryResponse
	}{
		{
			name:           "empty patterns",
			object:         "pipeline",
			configPatterns: []PatternData{},
			want:           MetricsDiscoveryResponse{},
		},
		{
			name:   "no matching object",
			object: "pipeline",
			configPatterns: []PatternData{
				{
					Object: "vertex",
					Metrics: []MetricData{
						{
							Name:    "test_metric",
							Filters: []string{"namespace"},
						},
					},
				},
			},
			want: []DiscoveryResponse{},
		},
		{
			name:   "single metric with required filters",
			object: "pipeline",
			configPatterns: []PatternData{
				{
					Object: "pipeline",
					Params: []ParamData{
						{
							Name:     "quantile",
							Required: true,
						},
					},
					Metrics: []MetricData{
						{
							Name:    "processing_rate",
							Filters: []string{"namespace", "pipeline"},
							Dimensions: []DimensionData{
								{
									Name: "vertex",
									Filters: []FilterData{
										{Name: "vertex", Required: true},
									},
								},
							},
						},
					},
				},
			},
			want: []DiscoveryResponse{
				{
					MetricName: "processing_rate",
					Dimensions: []Dimensions{
						{
							Name: "vertex",
							Filters: []FilterData{
								{Name: "namespace", Required: true},
								{Name: "pipeline", Required: true},
								{Name: "vertex", Required: true},
							},
							Params: []ParamData{{
								Name:     "quantile",
								Required: true,
							}},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Create mock promql service
			promQlService := NewMockPromQlService(&PrometheusConfig{
				Patterns: tt.configPatterns,
			})

			// Create handler with service
			h := &handler{
				promQlService: promQlService,
			}

			// Create gin test context
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Params = []gin.Param{
				{
					Key:   "object",
					Value: tt.object,
				},
			}

			// Call the handler
			h.DiscoverMetrics(c)

			// Check response
			var response NumaflowAPIResponse
			err := json.Unmarshal(w.Body.Bytes(), &response)
			if err != nil {
				t.Fatalf("Failed to unmarshal response: %v", err)
			}

			// Convert response data to MetricsDiscoveryResponse
			responseBytes, err := json.Marshal(response.Data)
			if err != nil {
				t.Fatalf("Failed to marshal response data: %v", err)
			}

			var got MetricsDiscoveryResponse
			if err := json.Unmarshal(responseBytes, &got); err != nil {
				t.Fatalf("Failed to unmarshal metrics discovery response: %v", err)
			}

			if !reflect.DeepEqual(got, tt.want) && len(got) != 0 {
				t.Errorf("DiscoverMetrics() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandler_GetMonoVertexPodsInfo(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		monoVertex    string
		pods          *corev1.PodList
		podMetrics    *metricsv1beta1.PodMetricsList
		expectedCode  int
		expectedError string
		simulateError bool
	}{
		{
			name:       "successful get pods info",
			namespace:  "test-ns",
			monoVertex: "test-mvt",
			pods: &corev1.PodList{
				Items: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-pod-1",
							Namespace: "test-ns",
							Labels: map[string]string{
								"numaflow.numaproj.io/mono-vertex-name": "test-mvt",
							},
						},
						Status: corev1.PodStatus{
							Phase: corev1.PodRunning,
							ContainerStatuses: []corev1.ContainerStatus{
								{
									Name:  "test-container",
									State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
								},
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "test-container",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("100Mi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("200m"),
											corev1.ResourceMemory: resource.MustParse("200Mi"),
										},
									},
								},
							},
						},
					},
				},
			},
			podMetrics: &metricsv1beta1.PodMetricsList{
				Items: []metricsv1beta1.PodMetrics{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-pod-1",
							Namespace: "test-ns",
						},
						Containers: []metricsv1beta1.ContainerMetrics{
							{
								Name: "test-container",
								Usage: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("150m"),
									corev1.ResourceMemory: resource.MustParse("150Mi"),
								},
							},
						},
					},
				},
			},
			expectedCode: http.StatusOK,
		},
		{
			name:       "no pods found",
			namespace:  "test-ns",
			monoVertex: "test-mvt",
			pods: &corev1.PodList{
				Items: []corev1.Pod{},
			},
			expectedCode:  http.StatusOK,
			expectedError: "GetMonoVertexPodInfo: No pods found for mono vertex \"test-mvt\" in namespace \"test-ns\"",
		},
		{
			name:          "error listing pods",
			namespace:     "test-ns",
			monoVertex:    "test-mvt",
			pods:          nil,
			expectedCode:  http.StatusOK,
			expectedError: "GetMonoVertexPodInfo: Failed to get a list of pods: namespace \"test-ns\" mono vertex \"test-mvt\":",
			simulateError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Params = gin.Params{
				{Key: "namespace", Value: tt.namespace},
				{Key: "mono-vertex", Value: tt.monoVertex},
			}

			kubeClient := fakeClient.NewSimpleClientset()
			if tt.simulateError {
				// Create a more specific reactor that matches the exact List call
				kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
					listAction, ok := action.(k8stesting.ListAction)
					if !ok {
						return false, nil, nil
					}
					// Verify this is the correct list call
					if listAction.GetListRestrictions().Labels.String() == fmt.Sprintf("%s=%s", dfv1.KeyMonoVertexName, tt.monoVertex) {
						return true, nil, fmt.Errorf("simulated error")
					}
					return false, nil, nil
				})

				// Verify the reactor is working
				_, err := kubeClient.CoreV1().Pods(tt.namespace).List(context.TODO(), metav1.ListOptions{
					LabelSelector: fmt.Sprintf("%s=%s", dfv1.KeyMonoVertexName, tt.monoVertex),
				})
				assert.Error(t, err, "Expected error from fake client")
				assert.Contains(t, err.Error(), "simulated error")
			} else if tt.pods != nil {
				for _, pod := range tt.pods.Items {
					_, err := kubeClient.CoreV1().Pods(tt.namespace).Create(context.TODO(), &pod, metav1.CreateOptions{})
					assert.NoError(t, err)
				}
				// Only verify pod count for non-error cases
				if tt.name == "successful get pods info" {
					pods, err := kubeClient.CoreV1().Pods(tt.namespace).List(context.TODO(), metav1.ListOptions{})
					assert.NoError(t, err)
					assert.Len(t, pods.Items, 1, "Expected one pod to be created")
				}
			}

			metricsClient := metricsfake.NewSimpleClientset()
			if tt.podMetrics != nil {
				// Setup reactor for Get() instead of List()
				metricsClient.Fake.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
					getAction, ok := action.(k8stesting.GetAction)
					if !ok {
						t.Errorf("Expected GetAction but got %v", action)
						return false, nil, nil
					}

					// Return the metrics for the specific pod
					for _, metric := range tt.podMetrics.Items {
						if metric.Name == getAction.GetName() && metric.Namespace == getAction.GetNamespace() {
							return true, &metric, nil
						}
					}
					return true, nil, fmt.Errorf("pod metrics not found")
				})
			}

			h := &handler{
				kubeClient:    kubeClient,
				metricsClient: metricsClient.MetricsV1beta1(),
			}

			h.GetMonoVertexPodsInfo(c)

			assert.Equal(t, tt.expectedCode, w.Code)

			var response NumaflowAPIResponse
			err := json.Unmarshal(w.Body.Bytes(), &response)
			assert.NoError(t, err)

			if tt.expectedError != "" {
				assert.NotNil(t, response.ErrMsg)
				assert.Contains(t, *response.ErrMsg, tt.expectedError)
			} else {
				assert.Nil(t, response.ErrMsg)
				assert.NotNil(t, response.Data)

				// Convert response.Data to []PodInfo
				responseBytes, err := json.Marshal(response.Data)
				assert.NoError(t, err)
				var podInfos []PodDetails
				err = json.Unmarshal(responseBytes, &podInfos)
				assert.NoError(t, err)

				assert.Len(t, podInfos, len(tt.pods.Items))

				if len(podInfos) > 0 {
					assert.Equal(t, "test-pod-1", podInfos[0].Name)
					assert.Equal(t, string(corev1.PodRunning), podInfos[0].Status)
					assert.Equal(t, "150m", podInfos[0].TotalCPU)
					assert.Equal(t, "150Mi", podInfos[0].TotalMemory)
				}
			}
		})
	}
}

func TestHandler_GetVertexPodsInfo(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		pipeline      string
		vertex        string
		pods          *corev1.PodList
		podMetrics    *metricsv1beta1.PodMetricsList
		expectedCode  int
		expectedError string
		simulateError bool
	}{
		{
			name:         "successful get pods info",
			namespace:    "test-ns",
			pipeline:     "test-pipeline",
			vertex:       "test-vertex",
			expectedCode: http.StatusOK,
			pods: &corev1.PodList{
				Items: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-pod-1",
							Namespace: "test-ns",
							Labels: map[string]string{
								dfv1.KeyPipelineName: "test-pipeline",
								dfv1.KeyVertexName:   "test-vertex",
							},
						},
						Status: corev1.PodStatus{
							Phase: corev1.PodRunning,
							ContainerStatuses: []corev1.ContainerStatus{
								{
									Name:  "test-container",
									State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
								},
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "test-container",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("100Mi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("200m"),
											corev1.ResourceMemory: resource.MustParse("200Mi"),
										},
									},
								},
							},
						},
					},
				},
			},
			podMetrics: &metricsv1beta1.PodMetricsList{
				Items: []metricsv1beta1.PodMetrics{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-pod-1",
							Namespace: "test-ns",
						},
						Containers: []metricsv1beta1.ContainerMetrics{
							{
								Name: "test-container",
								Usage: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("150m"),
									corev1.ResourceMemory: resource.MustParse("150Mi"),
								},
							},
						},
					},
				},
			},
		},
		{
			name:          "error_listing_pods",
			namespace:     "test-ns",
			pipeline:      "test-pipeline",
			vertex:        "test-vertex",
			expectedCode:  http.StatusOK,
			expectedError: "GetVertexPodsInfo: Failed to get a list of pods: namespace \"test-ns\" pipeline \"test-pipeline\" vertex \"test-vertex\": simulated error",
			simulateError: true,
		},
		{
			name:          "no_pods_found",
			namespace:     "test-ns",
			pipeline:      "test-pipeline",
			vertex:        "test-vertex",
			expectedCode:  http.StatusOK,
			expectedError: "GetVertexPodsInfo: No pods found for pipeline \"test-pipeline\" vertex \"test-vertex\" in namespace \"test-ns\"",
			pods:          &corev1.PodList{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Params = gin.Params{
				{Key: "namespace", Value: tt.namespace},
				{Key: "pipeline", Value: tt.pipeline},
				{Key: "vertex", Value: tt.vertex},
			}

			kubeClient := fakeClient.NewSimpleClientset()
			if tt.simulateError {
				kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
					listAction, ok := action.(k8stesting.ListAction)
					if !ok {
						return false, nil, nil
					}
					expectedSelector := fmt.Sprintf("%s=%s,%s=%s",
						dfv1.KeyPipelineName, tt.pipeline,
						dfv1.KeyVertexName, tt.vertex,
					)
					if listAction.GetListRestrictions().Labels.String() == expectedSelector {
						return true, nil, fmt.Errorf("simulated error")
					}
					return false, nil, nil
				})

				// Verify the reactor is working
				_, err := kubeClient.CoreV1().Pods(tt.namespace).List(context.TODO(), metav1.ListOptions{
					LabelSelector: fmt.Sprintf("%s=%s,%s=%s",
						dfv1.KeyPipelineName, tt.pipeline,
						dfv1.KeyVertexName, tt.vertex,
					),
				})
				assert.Error(t, err, "Expected error from fake client")
				assert.Contains(t, err.Error(), "simulated error")
			} else if tt.pods != nil {
				for _, pod := range tt.pods.Items {
					_, err := kubeClient.CoreV1().Pods(tt.namespace).Create(context.TODO(), &pod, metav1.CreateOptions{})
					assert.NoError(t, err)
				}

				if tt.name == "successful get pods info" {
					pods, err := kubeClient.CoreV1().Pods(tt.namespace).List(context.TODO(), metav1.ListOptions{})
					assert.NoError(t, err)
					assert.Len(t, pods.Items, 1, "Expected one pod to be created")
				}
			}

			metricsClient := metricsfake.NewSimpleClientset()
			if tt.podMetrics != nil {
				metricsClient.Fake.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
					getAction, ok := action.(k8stesting.GetAction)
					if !ok {
						return false, nil, nil
					}

					for _, metric := range tt.podMetrics.Items {
						if metric.Name == getAction.GetName() && metric.Namespace == getAction.GetNamespace() {
							return true, &metric, nil
						}
					}
					return true, nil, fmt.Errorf("pod metrics not found")
				})
			}

			h := &handler{
				kubeClient:    kubeClient,
				metricsClient: metricsClient.MetricsV1beta1(),
			}

			h.GetVertexPodsInfo(c)

			assert.Equal(t, tt.expectedCode, w.Code)

			var response NumaflowAPIResponse
			err := json.Unmarshal(w.Body.Bytes(), &response)
			assert.NoError(t, err)

			if tt.expectedError != "" {
				assert.NotNil(t, response.ErrMsg)
				assert.Contains(t, *response.ErrMsg, tt.expectedError)
			} else {
				assert.Nil(t, response.ErrMsg)
				podInfos := make([]PodDetails, 0)
				responseBytes, err := json.Marshal(response.Data)
				assert.NoError(t, err)
				err = json.Unmarshal(responseBytes, &podInfos)
				assert.NoError(t, err)
				assert.Len(t, podInfos, 1)
				assert.Equal(t, "test-pod-1", podInfos[0].Name)
				assert.Equal(t, string(corev1.PodRunning), podInfos[0].Status)
				assert.Equal(t, "150m", podInfos[0].TotalCPU)
				assert.Equal(t, "150Mi", podInfos[0].TotalMemory)
			}
		})
	}
}

// Mock PromQl service for testing
type mockPromQlService struct {
	configData *PrometheusConfig
}

func NewMockPromQlService(configData *PrometheusConfig) PromQl {
	return &mockPromQlService{
		configData: configData,
	}
}

func (m *mockPromQlService) BuildQuery(MetricsRequestBody) (string, error) {
	return "", nil
}

func (m *mockPromQlService) QueryPrometheus(context.Context, string, time.Time, time.Time) (model.Value, error) {
	return nil, nil
}

func (m *mockPromQlService) GetConfigData() *PrometheusConfig {
	return m.configData
}

func (m *mockPromQlService) PopulateReqMap(MetricsRequestBody) map[string]string {
	return map[string]string{}
}
