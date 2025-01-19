package v1

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

// MockPrometheusAPI is a mock implementation of the PrometheusAPI interface
type MockPrometheusAPI struct{}
type MockPrometheusClient struct{}

func (m *MockPrometheusClient) Do(ctx context.Context, req *http.Request) (*http.Response, []byte, error) {
	return nil, nil, nil
}

// QueryRange mock implementation
func (m *MockPrometheusAPI) QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error) {
	// Create a mock Prometheus API response
	mockResponse := model.Matrix{
		&model.SampleStream{
			Metric: model.Metric{"pipeline": "simple-pipeline"},
			Values: []model.SamplePair{
				{Timestamp: 1728364347, Value: 3442.72526560},
				{Timestamp: 1728364407, Value: 3446.17140174},
			},
		},
	}
	return mockResponse, nil, nil
}

func compareFilters(query1, query2 string) bool {
	//Extract the filter portions of the queries
	filters1 := extractfilters(query1)
	filters2 := extractfilters(query2)
	return reflect.DeepEqual(filters1, filters2)
}

// comparePrometheusQueries compares two Prometheus queries, ignoring the order of filters within the curly braces
func comparePrometheusQueries(query1, query2 string) bool {
	//Extract the filter portions of the queries
	filters1 := extractfilters(query1)
	filters2 := extractfilters(query2)
	//Compare the filter portions using reflect.DeepEqual, which ignores order
	if !reflect.DeepEqual(filters1, filters2) {
		return false // Filters don't match
	}

	//Remove filter portions from the queries
	query1 = removeFilters(query1)
	query2 = removeFilters(query2)

	//Normalize the remaining parts of the queries
	query1 = normalizeQuery(query1)
	query2 = normalizeQuery(query2)

	//Compare the normalized queries
	return cmp.Equal(query1, query2, cmpopts.IgnoreUnexported(struct{}{}))

}

func normalizeQuery(query string) string {
	// Remove extra whitespace and normalize case
	query = strings.TrimSpace(strings.ToLower(query))
	return query
}

// remove filters within {}
func removeFilters(query string) string {
	re := regexp.MustCompile(`\{(.*?)\}`)
	return re.ReplaceAllString(query, "")
}

// extractfilters extracts the key-value pairs within the curly braces
// from a Prometheus query using a regular expression.
func extractfilters(query string) map[string]string {
	re := regexp.MustCompile(`\{(.*?)\}`) // Regex to match content within curly braces
	match := re.FindStringSubmatch(query)

	if len(match) < 2 { // No match found
		return nil
	}

	filterstring := match[1] // Get the captured group (content within braces)
	filterPairs := strings.Split(filterstring, ",")
	filters := make(map[string]string)

	for _, pair := range filterPairs {
		parts := strings.Split(pair, "=")
		if len(parts) == 2 { // Ensure valid key-value pair
			filters[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	return filters
}

func Test_PopulateReqMap(t *testing.T) {
	t.Run("Map creation with all fields", func(t *testing.T) {
		requestBody := MetricsRequestBody{
			MetricName: "test_metric",
			Filters:    map[string]string{"filter1": "value1", "filter2": "value2"},
			Dimension:  "group1",
			Duration:   "5m",
			Quantile:   "0.95",
			StartTime:  "2024-10-08T12:00:00Z",
			EndTime:    "2024-10-08T13:00:00Z",
		}
		expectedMap := map[string]string{
			"$metric_name": "test_metric",
			"$filters":     "filter1= \"value1\", filter2= \"value2\"",
			"$dimension":   "group1",
			"$quantile":    "0.95",
			"$duration":    "5m",
		}

		promQlService := &PromQlService{}
		actualMap := promQlService.PopulateReqMap(requestBody)

		assert.Equal(t, actualMap["$metric_name"], expectedMap["$metric_name"])
		assert.Equal(t, actualMap["$quantile"], expectedMap["$quantile"])
		assert.Equal(t, actualMap["$duration"], expectedMap["$duration"])
		assert.Equal(t, actualMap["$dimension"], expectedMap["$dimension"])
		if !compareFilters(expectedMap["$filters"], actualMap["$filters"]) {
			t.Errorf("filters do not match")
		}
	})

	t.Run("Mapping with empty fields", func(t *testing.T) {
		requestBody := MetricsRequestBody{
			MetricName: "test_metric",
		}
		expectedMap := map[string]string{
			"$metric_name": "test_metric",
			"$filters":     "",
			"$dimension":   "",
			"$quantile":    "",
			"$duration":    "",
		}

		promQlService := &PromQlService{}
		actualMap := promQlService.PopulateReqMap(requestBody)
		assert.Equal(t, actualMap["$metric_name"], expectedMap["$metric_name"])
		assert.Equal(t, actualMap["$quantile"], expectedMap["$quantile"])
		assert.Equal(t, actualMap["$duration"], expectedMap["$duration"])
		assert.Equal(t, actualMap["$dimension"], expectedMap["$dimension"])

		if !compareFilters(expectedMap["$filters"], actualMap["$filters"]) {
			t.Errorf("filters do not match")
		}
	})
}
func Test_PromQueryBuilder(t *testing.T) {
	// tests for histogram
	var histogram_service = &PromQlService{
		PlaceHolders: map[string]map[string][]string{
			"test_metric": {
				"test_dimension": {"$quantile", "$dimension", "$metric_name", "$filters", "$duration"},
			},
		},
		Expression: map[string]map[string]string{
			"test_metric": {
				"test_dimension": "histogram_quantile($quantile, sum by($dimension,le) (rate($metric_name{$filters}[$duration])))",
			},
		},
	}

	histogram_metrics_tests := []struct {
		name          string
		requestBody   MetricsRequestBody
		expectedQuery string
		expectError   bool
	}{
		{
			name: "Successful histogram metrics template substitution",
			requestBody: MetricsRequestBody{
				MetricName: "test_metric",
				Quantile:   "0.90",
				Duration:   "5m",
				Dimension:  "test_dimension",
				Filters: map[string]string{
					"namespace": "test_namespace",
					"mvtx_name": "test-mono-vertex",
					"pod":       "test-pod",
				},
			},
			expectedQuery: `histogram_quantile(0.90, sum by(test_dimension,le) (rate(test_metric{namespace= "test_namespace", mvtx_name= "test-mono-vertex", pod= "test-pod"}[5m])))`,
		},
		{
			name: "Missing placeholder in req",
			requestBody: MetricsRequestBody{
				MetricName: "test_metric",
				Duration:   "5m",
				Dimension:  "test_dimension",
				Filters: map[string]string{
					"namespace": "test_namespace",
					"mvtx_name": "test-mono-vertex",
					"pod":       "test-pod",
				},
			},
			expectError: true,
		},
		{
			name: "Missing metric name in service config",
			requestBody: MetricsRequestBody{
				MetricName: "test_bucket",
				Duration:   "5m",
				Dimension:  "test_dimension",
				Filters: map[string]string{
					"namespace": "test_namespace",
					"mvtx_name": "test-mono-vertex",
					"pod":       "test-pod",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range histogram_metrics_tests {
		t.Run(tt.name, func(t *testing.T) {
			actualQuery, err := histogram_service.BuildQuery(tt.requestBody)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if !comparePrometheusQueries(tt.expectedQuery, actualQuery) {
					t.Errorf("Prometheus queries do not match.\nExpected: %s\nGot: %s", tt.expectedQuery, actualQuery)
				} else {
					t.Log("Prometheus queries match!")
				}
			}
		})
	}

	// tests for counter metrics
	var counter_service = &PromQlService{
		PlaceHolders: map[string]map[string][]string{
			"forwarder_data_read_total": {
				"vertex": {"$duration", "$dimension", "$metric_name", "$filters"},
			},
		},
		Expression: map[string]map[string]string{
			"forwarder_data_read_total": {
				"vertex": "sum(rate($metric_name{$filters}[$duration])) by ($dimension)",
			},
		},
	}

	counter_metrics_tests := []struct {
		name          string
		requestBody   MetricsRequestBody
		expectedQuery string
		expectError   bool
	}{
		{
			name: "Successful counter metrics template substitution",
			requestBody: MetricsRequestBody{
				MetricName: "forwarder_data_read_total",
				Duration:   "5m",
				Dimension:  "vertex",
				Filters: map[string]string{
					"namespace": "test_namespace",
					"pipeline":  "test_pipeline",
					"vertex":    "test_vertex",
				},
			},
			expectedQuery: `sum(rate(forwarder_data_read_total{namespace= "test_namespace", pipeline= "test_pipeline", vertex= "test_vertex"}[5m])) by (vertex)`,
		},
		{
			name: "Missing metric name in service config",
			requestBody: MetricsRequestBody{
				MetricName: "non_existent_metric",
				Duration:   "5m",
				Dimension:  "vertex",
				Filters: map[string]string{
					"namespace": "test_namespace",
					"pipeline":  "test_pipeline",
					"vertex":    "test_vertex",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range counter_metrics_tests {
		t.Run(tt.name, func(t *testing.T) {
			actualQuery, err := counter_service.BuildQuery(tt.requestBody)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if !comparePrometheusQueries(tt.expectedQuery, actualQuery) {
					t.Errorf("Prometheus queries do not match.\nExpected: %s\nGot: %s", tt.expectedQuery, actualQuery)
				} else {
					t.Log("Prometheus queries match!")
				}
			}
		})
	}

	// tests for mono-vertex gauge metrics
	var gauge_service = &PromQlService{
		PlaceHolders: map[string]map[string][]string{
			"monovtx_pending": {
				"mono-vertex": {"$dimension", "$metric_name", "$filters"},
			},
		},
		Expression: map[string]map[string]string{
			"monovtx_pending": {
				"mono-vertex": "sum($metric_name{$filters}) by ($dimension, period)",
			},
		},
	}

	gauge_metrics_tests := []struct {
		name          string
		requestBody   MetricsRequestBody
		expectedQuery string
		expectError   bool
	}{
		{
			name: "Successful gauge metrics template substitution",
			requestBody: MetricsRequestBody{
				MetricName: "monovtx_pending",
				Dimension:  "mono-vertex",
				Filters: map[string]string{
					"namespace": "test_namespace",
					"mvtx_name": "test_mvtx",
					"period":    "5m",
				},
			},
			expectedQuery: `sum(monovtx_pending{namespace= "test_namespace", mvtx_name= "test_mvtx", period= "5m"}) by (mvtx_name, period)`,
		},
		{
			name: "Missing metric name in service config",
			requestBody: MetricsRequestBody{
				MetricName: "non_existent_metric",
				Dimension:  "mono-vertex",
				Filters: map[string]string{
					"namespace": "test_namespace",
					"mvtx_name": "test_mvtx",
					"period":    "5m",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range gauge_metrics_tests {
		t.Run(tt.name, func(t *testing.T) {
			actualQuery, err := gauge_service.BuildQuery(tt.requestBody)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if !comparePrometheusQueries(tt.expectedQuery, actualQuery) {
					t.Errorf("Prometheus queries do not match.\nExpected: %s\nGot: %s", tt.expectedQuery, actualQuery)
				} else {
					t.Log("Prometheus queries match!")
				}
			}
		})
	}

	// tests for pipeline gauge metrics
	var pl_gauge_service = &PromQlService{
		PlaceHolders: map[string]map[string][]string{
			"vertex_pending_messages": {
				"vertex": {"$dimension", "$metric_name", "$filters"},
			},
		},
		Expression: map[string]map[string]string{
			"vertex_pending_messages": {
				"vertex": "sum($metric_name{$filters}) by ($dimension, period)",
			},
		},
	}

	pl_gauge_metrics_tests := []struct {
		name          string
		requestBody   MetricsRequestBody
		expectedQuery string
		expectError   bool
	}{
		{
			name: "Successful pipeline gauge metrics template substitution",
			requestBody: MetricsRequestBody{
				MetricName: "vertex_pending_messages",
				Dimension:  "vertex",
				Filters: map[string]string{
					"namespace": "test_namespace",
					"pipeline":  "test_pipeline",
					"vertex":    "test_vertex",
					"period":    "5m",
				},
			},
			expectedQuery: `sum(vertex_pending_messages{namespace= "test_namespace", pipeline= "test_pipeline", vertex= "test_vertex", period= "5m"}) by (vertex, period)`,
		},
		{
			name: "Missing metric name in service config",
			requestBody: MetricsRequestBody{
				MetricName: "non_existent_metric",
				Dimension:  "mono-vertex",
				Filters: map[string]string{
					"namespace": "test_namespace",
					"pipeline":  "test_pipeline",
					"vertex":    "test_vertex",
					"period":    "5m",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range pl_gauge_metrics_tests {
		t.Run(tt.name, func(t *testing.T) {
			actualQuery, err := pl_gauge_service.BuildQuery(tt.requestBody)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if !comparePrometheusQueries(tt.expectedQuery, actualQuery) {
					t.Errorf("Prometheus queries do not match.\nExpected: %s\nGot: %s", tt.expectedQuery, actualQuery)
				} else {
					t.Log("Prometheus queries match!")
				}
			}
		})
	}
}

func Test_QueryPrometheus(t *testing.T) {
	t.Run("Successful histogram query", func(t *testing.T) {
		mockAPI := &MockPrometheusAPI{}
		promQlService := &PromQlService{
			PrometheusClient: &Prometheus{
				Api: mockAPI,
			},
		}
		query := `histogram_quantile(0.99, sum by (pipeline, le) (rate(forwarder_udf_processing_time_bucket{namespace="default", pipeline="simple-pipeline"}[5m])))`
		startTime := time.Now().Add(-30 * time.Minute)
		endTime := time.Now()

		ctx := context.Background()
		result, err := promQlService.QueryPrometheus(ctx, query, startTime, endTime)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// for query range , response should be a matrix
		matrix, ok := result.(model.Matrix)
		assert.True(t, ok)
		assert.Equal(t, 1, matrix.Len())
	})

	t.Run("Successful counter query", func(t *testing.T) {
		mockAPI := &MockPrometheusAPI{}
		promQlService := &PromQlService{
			PrometheusClient: &Prometheus{
				Api: mockAPI,
			},
		}
		query := `sum(rate(forwarder_data_read_total{namespace="default", pipeline="test-pipeline"}[5m])) by (vertex)`
		startTime := time.Now().Add(-30 * time.Minute)
		endTime := time.Now()

		ctx := context.Background()
		result, err := promQlService.QueryPrometheus(ctx, query, startTime, endTime)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// for query range , response should be a matrix
		matrix, ok := result.(model.Matrix)
		assert.True(t, ok)
		assert.Equal(t, 1, matrix.Len())
	})

	t.Run("Successful mono-vertex gauge query", func(t *testing.T) {
		mockAPI := &MockPrometheusAPI{}
		promQlService := &PromQlService{
			PrometheusClient: &Prometheus{
				Api: mockAPI,
			},
		}
		query := `sum(monovtx_pending{namespace="default", mvtx_name="test-mvtx", period="5m"}) by (mvtx_name, period)`
		startTime := time.Now().Add(-30 * time.Minute)
		endTime := time.Now()

		ctx := context.Background()
		result, err := promQlService.QueryPrometheus(ctx, query, startTime, endTime)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// for query range , response should be a matrix
		matrix, ok := result.(model.Matrix)
		assert.True(t, ok)
		assert.Equal(t, 1, matrix.Len())
	})

	t.Run("Successful pipeline gauge query", func(t *testing.T) {
		mockAPI := &MockPrometheusAPI{}
		promQlService := &PromQlService{
			PrometheusClient: &Prometheus{
				Api: mockAPI,
			},
		}
		query := `sum(vertex_pending_messages{namespace="default", pipeline="test-pipeline", vertex="test-vertex", period="5m"}) by (vertex, period)`
		startTime := time.Now().Add(-30 * time.Minute)
		endTime := time.Now()

		ctx := context.Background()
		result, err := promQlService.QueryPrometheus(ctx, query, startTime, endTime)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// for query range , response should be a matrix
		matrix, ok := result.(model.Matrix)
		assert.True(t, ok)
		assert.Equal(t, 1, matrix.Len())
	})

	t.Run("Prometheus client is nil", func(t *testing.T) {
		service := &PromQlService{
			PrometheusClient: nil,
		}
		_, err := service.QueryPrometheus(context.Background(), "up", time.Now().Add(-10*time.Minute), time.Now())
		if err == nil {
			t.Fatalf("expected an error, got nil")
		}
		expectedError := "prometheus client is not defined"
		if err.Error() != expectedError {
			t.Errorf("expected error %v, got %v", expectedError, err)
		}
	})
}

func TestGetConfigData(t *testing.T) {
	tests := []struct {
		name     string
		service  *PromQlService
		expected *PrometheusConfig
	}{
		{
			name: "returns nil when config is not set",
			service: &PromQlService{
				ConfigData: nil,
			},
			expected: nil,
		},
		{
			name: "returns config when config is set",
			service: &PromQlService{
				ConfigData: &PrometheusConfig{
					ServerUrl: "http://test.com",
				},
			},
			expected: &PrometheusConfig{
				ServerUrl: "http://test.com",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.service.GetConfigData()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDisableMetricsChart(t *testing.T) {
	tests := []struct {
		name     string
		service  *PromQlService
		expected bool
	}{
		{
			name: "returns true when both config and client are nil",
			service: &PromQlService{
				ConfigData:       nil,
				PrometheusClient: nil,
			},
			expected: true,
		},
		{
			name: "returns true when only config is nil",
			service: &PromQlService{
				ConfigData:       nil,
				PrometheusClient: &Prometheus{},
			},
			expected: true,
		},
		{
			name: "returns true when only client is nil",
			service: &PromQlService{
				ConfigData:       &PrometheusConfig{},
				PrometheusClient: nil,
			},
			expected: true,
		},
		{
			name: "returns false when both config and client are set",
			service: &PromQlService{
				ConfigData:       &PrometheusConfig{},
				PrometheusClient: &Prometheus{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.service.DisableMetricsChart()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewPromQlServiceObject(t *testing.T) {
	// Test cases with different scenarios
	tests := []struct {
		name                 string
		mockConfig           *PrometheusConfig
		mockConfigErr        error
		mockClient           *Prometheus
		mockClientErr        error
		expectedExpr         map[string]map[string]string
		expectedPlaceholders map[string]map[string][]string
	}{
		{
			name: "Successful initialization",
			mockConfig: &PrometheusConfig{
				ServerUrl: "http://prometheus:9090",
				Patterns: []Pattern{
					{
						Expression: `sum(rate($metric{label="$label"}[$interval])) by ($groupBy)`,
						Metrics: []Metric{
							{
								Name: "metric_1",
								Dimensions: []Dimension{
									{Name: "label", Expression: ""},
									{Name: "groupBy", Expression: "$groupBy"},
								},
							},
						},
					},
				},
			},
			mockClient: &Prometheus{
				Client: &MockPrometheusClient{},
				Api:    &MockPrometheusAPI{},
			},
			expectedExpr: map[string]map[string]string{
				"metric_1": {
					"label":   `sum(rate($metric{label="$label"}[$interval])) by ($groupBy)`,
					"groupBy": "$groupBy",
				},
			},
			expectedPlaceholders: map[string]map[string][]string{
				"metric_1": {
					"label":   []string{"$metric", "$label", "$interval", "$groupBy"},
					"groupBy": []string{"$groupBy"},
				},
			},
		},
		{
			name:                 "Error loading config",
			mockConfigErr:        fmt.Errorf("config error"),
			expectedExpr:         nil,
			expectedPlaceholders: nil,
		},
		{
			name: "Error creating client",
			mockConfig: &PrometheusConfig{
				ServerUrl: "http://prometheus:9090",
			},
			mockClientErr:        fmt.Errorf("client error"),
			expectedExpr:         nil,
			expectedPlaceholders: nil,
		},
		{
			name:                 "Empty config file",
			mockConfig:           &PrometheusConfig{}, // Empty config
			expectedExpr:         nil,
			expectedPlaceholders: nil,
		},
		{
			name:                 "Invalid config file format",
			mockConfigErr:        fmt.Errorf("yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `invalid...` into main.PrometheusConfig"), // Simulate invalid YAML error
			expectedExpr:         nil,
			expectedPlaceholders: nil,
		},
		{
			name: "Missing server URL in config",
			mockConfig: &PrometheusConfig{
				Patterns: []Pattern{ // ServerUrl is missing
					{
						Expression: `sum(rate($metric{label="$label"}[$interval])) by ($groupBy)`,
						Metrics: []Metric{
							{
								Name: "metric_1",
								Dimensions: []Dimension{
									{Name: "label", Expression: ""},
									{Name: "groupBy", Expression: "$groupBy"},
								},
							},
						},
					},
				},
			},
			mockClientErr:        fmt.Errorf("prometheus server url is not set"), // Expect client creation error
			expectedExpr:         nil,
			expectedPlaceholders: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock loadPrometheusMetricConfig and newPrometheusClient
			originalLoadConfig := loadPrometheusMetricConfig
			originalNewClient := newPrometheusClient
			defer func() {
				loadPrometheusMetricConfig = originalLoadConfig
				newPrometheusClient = originalNewClient
			}()

			loadPrometheusMetricConfig = func() (*PrometheusConfig, error) {
				return tt.mockConfig, tt.mockConfigErr
			}

			newPrometheusClient = func(url string) (*Prometheus, error) {
				return tt.mockClient, tt.mockClientErr
			}

			service, err := NewPromQlServiceObject()
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			promService := service.(*PromQlService)

			if tt.mockConfigErr != nil || tt.mockClientErr != nil || tt.mockConfig.ServerUrl == "" {
				// If there's an error in loading config or creating client,
				//  expressions and placeholders should be nil (empty maps)
				if len(promService.Expression) != 0 { // Check for empty map
					t.Errorf("Expressions mismatch. Got: %v, Want: %v", promService.Expression, map[string]map[string]string{})
				}
				if len(promService.PlaceHolders) != 0 { // Check for empty map
					t.Errorf("Placeholders mismatch. Got: %v, Want: %v", promService.PlaceHolders, map[string]map[string][]string{})
				}
			} else {
				// Otherwise, compare with the expected values
				if !reflect.DeepEqual(promService.Expression, tt.expectedExpr) {
					t.Errorf("Expressions mismatch. Got: %v, Want: %v", promService.Expression, tt.expectedExpr)
				}

				if !reflect.DeepEqual(promService.PlaceHolders, tt.expectedPlaceholders) {
					t.Errorf("Placeholders mismatch. Got: %v, Want: %v", promService.PlaceHolders, tt.expectedPlaceholders)
				}
			}
		})
	}
}
