package v1

import (
	"context"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

// MockPrometheusAPI is a mock implementation of the PrometheusAPI interface
type MockPrometheusAPI struct{}

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

// comparePrometheusQueries compares two Prometheus queries, ignoring the order of filters within the curly braces
func comparePrometheusQueries(query1, query2 string) bool {
	// Extract the filter portions of the queries
	filters1 := extractfilters(query1)
	filters2 := extractfilters(query2)
	// Compare the filter portions using reflect.DeepEqual, which ignores order
	return reflect.DeepEqual(filters1, filters2)
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
		if !comparePrometheusQueries(expectedMap["$filters"], actualMap["$filters"]) {
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

		if !comparePrometheusQueries(expectedMap["$filters"], actualMap["$filters"]) {
			t.Errorf("filters do not match")
		}
	})
}
func Test_PromQueryBuilder(t *testing.T) {
	var service = &PromQlService{
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

	tests := []struct {
		name          string
		requestBody   MetricsRequestBody
		expectedQuery string
		expectError   bool
	}{
		{
			name: "Successful template substitution",
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
			expectedQuery: `histogram_quantile(0.90, sum by(test_dimension,le) (rate(test_bucket{namespace= "test_namespace", mvtx_name= "test-mono-vertex", pod= "test-pod"}[5m])))`,
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualQuery, err := service.BuildQuery(tt.requestBody)
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
	t.Run("Successful query", func(t *testing.T) {
		mockAPI := &MockPrometheusAPI{}
		promQlService := &PromQlService{
			Prometheus: &Prometheus{
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
	t.Run("Prometheus is nil", func(t *testing.T) {
		service := &PromQlService{
			Prometheus: nil,
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
