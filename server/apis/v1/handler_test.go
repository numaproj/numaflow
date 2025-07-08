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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
	metricsclientv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"

	"github.com/gin-gonic/gin"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	fakeClient "k8s.io/client-go/kubernetes/fake"
)

var (
	validPatchSpec   = `{"spec": {"lifecycle": {"desiredPhase": "Paused"}}}`
	invalidPatchSpec = `{"spec": {"limits": {"readTimeout": "5s"}}}`
)

// Mock PromQl service for testing
type mockPromQlService struct {
	configData *PrometheusConfig
}

func NewmockPromQlService(configData *PrometheusConfig) PromQl {
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

func (m *mockPromQlService) DisableMetricsChart() bool {
	return m.configData == nil
}

type MockMetricsClient struct {
	podMetrics *metricsv1beta1.PodMetrics
}

func (m *MockMetricsClient) PodMetricses(namespace string) metricsclientv1beta1.PodMetricsInterface {
	return &MockPodMetricsInterface{podMetrics: m.podMetrics}
}
func (m *MockMetricsClient) NodeMetricses() metricsclientv1beta1.NodeMetricsInterface {
	return &MockNodeMetricsInterface{}
}

func (m *MockMetricsClient) RESTClient() rest.Interface {
	return nil
}

type MockPodMetricsInterface struct {
	podMetrics *metricsv1beta1.PodMetrics
}
type MockNodeMetricsInterface struct{}

func (m *MockNodeMetricsInterface) Get(ctx context.Context, name string, opts metav1.GetOptions) (*metricsv1beta1.NodeMetrics, error) {
	// Mock the node metrics data here
	return nil, nil
}

func (m *MockNodeMetricsInterface) List(ctx context.Context, opts metav1.ListOptions) (*metricsv1beta1.NodeMetricsList, error) {
	// Mock the node metrics data here
	return nil, nil
}

func (m *MockNodeMetricsInterface) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return nil, nil
}

func (m *MockPodMetricsInterface) Get(ctx context.Context, name string, opts metav1.GetOptions) (*metricsv1beta1.PodMetrics, error) {
	// Mock the pod metrics data here
	if m.podMetrics == nil {
		return nil, fmt.Errorf("pod metrics not found")
	}
	return m.podMetrics, nil
}

func (m *MockPodMetricsInterface) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	// Mock the pod metrics data here
	return nil, nil
}

func (m *MockPodMetricsInterface) List(ctx context.Context, opts metav1.ListOptions) (*metricsv1beta1.PodMetricsList, error) {
	// Mock the pod metrics data here
	return nil, nil
}

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
		configPatterns []Pattern
		want           MetricsDiscoveryResponse
	}{
		{
			name:           "empty patterns",
			object:         "vertex",
			configPatterns: []Pattern{},
			want:           MetricsDiscoveryResponse{},
		},
		{
			name:   "no matching object",
			object: "pipeline", // case where request has a different object than pattern's list of objects
			configPatterns: []Pattern{
				{
					Objects: []string{"vertex"},
					Metrics: []Metric{
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
			object: "vertex",
			configPatterns: []Pattern{
				{
					Objects: []string{"vertex"},
					Params: []Params{
						{
							Name:     "quantile",
							Required: true,
						},
					},
					Metrics: []Metric{
						{
							Name:    "processing_rate",
							Filters: []string{"namespace"},
							Dimensions: []Dimension{
								{
									Name: "vertex",
									Filters: []Filter{
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
							Filters: []Filter{
								{Name: "namespace", Required: true},
								{Name: "vertex", Required: true},
							},
							Params: []Params{{
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
			promQlServiceObj := NewmockPromQlService(&PrometheusConfig{
				Patterns: tt.configPatterns,
			})

			// Create handler with service
			h := &handler{
				promQlServiceObj: promQlServiceObj,
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

func TestHandler_GetPodDetails(t *testing.T) {

	mockPodMetrics := &metricsv1beta1.PodMetrics{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test-namespace",
		},
		Containers: []metricsv1beta1.ContainerMetrics{
			{
				Name: "container-1",
				Usage: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("200Mi"),
				},
			},
			{
				Name: "container-2",
				Usage: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("150m"),
					corev1.ResourceMemory: resource.MustParse("300Mi"),
				},
			},
		},
	}

	h := handler{
		metricsClient: &MockMetricsClient{podMetrics: mockPodMetrics},
	}
	now := metav1.NewTime(time.Now()) // Initialize now here

	tests := []struct {
		name            string
		pod             corev1.Pod
		expectedDetails PodDetails
		expectedErr     error
	}{
		{
			name: "Successful pod details retrieval",
			pod: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "test-namespace",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name:         "container-1",
							ContainerID:  "docker://container-1-id",
							State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
							RestartCount: 0,
						},
						{
							Name:  "container-2",
							State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "WaitingReason", Message: "WaitingMessage"}},
						},
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "container-1",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI),
									corev1.ResourceMemory: *resource.NewQuantity(200*1024*1024,
										resource.BinarySI),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
									corev1.ResourceMemory: *resource.NewQuantity(400*1024*1024, resource.BinarySI),
								},
							},
						},
						{
							Name: "container-2",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    *resource.NewMilliQuantity(150, resource.DecimalSI),
									corev1.ResourceMemory: *resource.NewQuantity(300*1024*1024, resource.BinarySI),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
									corev1.ResourceMemory: *resource.NewQuantity(600*1024*1024, resource.BinarySI),
								},
							},
						},
					},
				},
			},
			expectedDetails: PodDetails{
				Name:        "test-pod",
				Status:      "Running",
				Message:     "",
				Reason:      "",
				TotalCPU:    "250m",
				TotalMemory: "500Mi",
				ContainerDetailsMap: map[string]ContainerDetails{
					"container-1": {
						Name:                    "container-1",
						ID:                      "docker://container-1-id",
						State:                   "Running",
						RestartCount:            0,
						LastStartedAt:           now.Format(time.RFC3339),
						RequestedCPU:            "100m",
						RequestedMemory:         "200Mi",
						LimitCPU:                "200m",
						LimitMemory:             "400Mi",
						TotalCPU:                "100m",
						TotalMemory:             "200Mi",
						LastTerminationReason:   "",
						LastTerminationMessage:  "",
						LastTerminationExitCode: nil,
						WaitingReason:           "",
						WaitingMessage:          "",
					},
					"container-2": {
						Name:                    "container-2",
						State:                   "Waiting",
						RestartCount:            0,
						WaitingReason:           "WaitingReason",
						WaitingMessage:          "WaitingMessage",
						RequestedCPU:            "150m",
						RequestedMemory:         "300Mi",
						LimitCPU:                "300m",
						LimitMemory:             "600Mi",
						TotalCPU:                "150m",
						TotalMemory:             "300Mi",
						LastTerminationReason:   "",
						LastTerminationMessage:  "",
						LastTerminationExitCode: nil,
						LastStartedAt:           "",
						ID:                      "",
					},
				},
			},
			expectedErr: nil,
		},
		// Add more test cases for different scenarios:
		// - Pod with terminated containers
		// - Pod with no container statuses
		// - Error getting pod metrics
		// ...
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podDetails, err := h.getPodDetails(tt.pod)

			assert.Equal(t, tt.expectedErr, err)
			assert.Equal(t, tt.expectedDetails, podDetails)
		})
	}
}

func TestHandler_GetContainerDetails(t *testing.T) {
	now := metav1.NewTime(time.Now())

	tests := []struct {
		name           string
		pod            corev1.Pod
		expectedResult map[string]ContainerDetails
	}{
		{
			name: "Pod with running and waiting containers",
			pod: corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name:         "running-container",
							ContainerID:  "docker://abc123",
							RestartCount: 2,
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{
									StartedAt: now,
								},
							},
						},
						{
							Name:         "waiting-container",
							ContainerID:  "docker://def456",
							RestartCount: 1,
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "ImagePullBackOff",
									Message: "Back-off pulling image",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Reason:   "Error",
									Message:  "Container crashed",
									ExitCode: 1,
								},
							},
						},
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "running-container",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("200Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("400Mi"),
								},
							},
						},
						{
							Name: "waiting-container",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("150m"),
									corev1.ResourceMemory: resource.MustParse("300Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("300m"),
									corev1.ResourceMemory: resource.MustParse("600Mi"),
								},
							},
						},
					},
				},
			},
			expectedResult: map[string]ContainerDetails{
				"running-container": {
					Name:                    "running-container",
					ID:                      "docker://abc123",
					State:                   "Running",
					RestartCount:            2,
					LastStartedAt:           now.Format(time.RFC3339),
					RequestedCPU:            "100m",
					RequestedMemory:         "200Mi",
					LimitCPU:                "200m",
					LimitMemory:             "400Mi",
					WaitingReason:           "",
					WaitingMessage:          "",
					LastTerminationReason:   "",
					LastTerminationMessage:  "",
					LastTerminationExitCode: nil,
				},
				"waiting-container": {
					Name:                    "waiting-container",
					ID:                      "docker://def456",
					State:                   "Waiting",
					RestartCount:            1,
					LastStartedAt:           "",
					RequestedCPU:            "150m",
					RequestedMemory:         "300Mi",
					LimitCPU:                "300m",
					LimitMemory:             "600Mi",
					WaitingReason:           "ImagePullBackOff",
					WaitingMessage:          "Back-off pulling image",
					LastTerminationReason:   "Error",
					LastTerminationMessage:  "Container crashed",
					LastTerminationExitCode: func() *int32 { i := int32(1); return &i }(),
				},
			},
		},
		{
			name: "Pod with no container statuses but with spec",
			pod: corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "spec-only-container",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("200Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("400Mi"),
								},
							},
						},
					},
				},
			},
			expectedResult: map[string]ContainerDetails{
				"spec-only-container": {
					Name:                    "spec-only-container",
					RequestedCPU:            "100m",
					RequestedMemory:         "200Mi",
					LimitCPU:                "200m",
					LimitMemory:             "400Mi",
					LastTerminationExitCode: nil,
				},
			},
		},
		{
			name:           "Empty pod",
			pod:            corev1.Pod{},
			expectedResult: map[string]ContainerDetails{},
		},
	}

	h := &handler{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.getContainerDetails(tt.pod)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestHandler_GetContainerStatus(t *testing.T) {
	h := &handler{}

	tests := []struct {
		name           string
		containerState corev1.ContainerState
		expectedStatus string
	}{
		{
			name: "Running container",
			containerState: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{
					StartedAt: metav1.NewTime(time.Now()),
				},
			},
			expectedStatus: "Running",
		},
		{
			name: "Waiting container",
			containerState: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Reason:  "ImagePullBackOff",
					Message: "Back-off pulling image",
				},
			},
			expectedStatus: "Waiting",
		},
		{
			name: "Terminated container",
			containerState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 1,
					Reason:   "Error",
					Message:  "Container crashed",
				},
			},
			expectedStatus: "Terminated",
		},
		{
			name:           "Empty container state",
			containerState: corev1.ContainerState{},
			expectedStatus: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := h.getContainerStatus(tt.containerState)
			assert.Equal(t, tt.expectedStatus, status)
		})
	}
}

func TestHandler_IsNotSidecarContainer(t *testing.T) {
	handler := &handler{}

	tests := []struct {
		name          string
		containerName string
		pod           corev1.Pod
		expected      bool
	}{
		{
			name:          "Init container with Always restart policy",
			containerName: "init-container-1",
			pod: corev1.Pod{
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Name:          "init-container-1",
							RestartPolicy: func() *corev1.ContainerRestartPolicy { p := corev1.ContainerRestartPolicyAlways; return &p }(),
						},
					},
				},
			},
			expected: false,
		},
		{
			name:          "Container not in init containers",
			containerName: "app-container",
			pod: corev1.Pod{
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Name: "init-container-1",
						},
					},
				},
			},
			expected: true,
		},
		{
			name:          "Init container with nil RestartPolicy",
			containerName: "init-container-3",
			pod: corev1.Pod{
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Name:          "init-container-3",
							RestartPolicy: nil,
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.isNotSidecarContainer(tt.containerName, tt.pod)
			assert.Equal(t, tt.expected, result)
		})
	}
}
