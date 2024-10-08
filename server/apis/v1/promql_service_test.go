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

// comparePrometheusQueries compares two Prometheus queries, ignoring the order of labels within the curly braces
func comparePrometheusQueries(query1, query2 string) bool {
	// Extract the label portions of the queries
	labels1 := extractLabels(query1)
	labels2 := extractLabels(query2)
	// Compare the label portions using reflect.DeepEqual, which ignores order
	return reflect.DeepEqual(labels1, labels2)
}

// extractLabels extracts the key-value pairs within the curly braces
// from a Prometheus query using a regular expression.
func extractLabels(query string) map[string]string {
	re := regexp.MustCompile(`\{(.*?)\}`) // Regex to match content within curly braces
	match := re.FindStringSubmatch(query)

	if len(match) < 2 { // No match found
		return nil
	}

	labelString := match[1] // Get the captured group (content within braces)
	labelPairs := strings.Split(labelString, ",")
	labels := make(map[string]string)

	for _, pair := range labelPairs {
		parts := strings.Split(pair, "=")
		if len(parts) == 2 { // Ensure valid key-value pair
			labels[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	return labels
}

func Test_PopulateReqMap(t *testing.T) {
	t.Run("Map creation with all fields", func(t *testing.T) {
		requestBody := MetricsRequestBody{
			PatternName:   "test_pattern",
			MetricName:    "test_metric",
			FilterLabels:  map[string]string{"label1": "value1", "label2": "value2"},
			GroupByLabels: []string{"group1", "group2"},
			Duration:      "5m",
			Quantile:      "0.95",
			StartTime:     "2024-10-08T12:00:00Z",
			EndTime:       "2024-10-08T13:00:00Z",
		}
		expectedMap := map[string]string{
			"$metric_name":         "test_metric",
			"$filter_labels":       "label1= \"value1\", label2= \"value2\"",
			"$group_by_labels":     "group1, group2",
			"$quantile_percentile": "0.95",
			"$duration":            "5m",
		}

		promQlService := &PromQlService{}
		actualMap := promQlService.PopulateReqMap(requestBody)

		assert.Equal(t, actualMap["$metric_name"], expectedMap["$metric_name"])
		assert.Equal(t, actualMap["$quantile_percentile"], expectedMap["$quantile_percentile"])
		assert.Equal(t, actualMap["$duration"], expectedMap["$duration"])

		if !comparePrometheusQueries(expectedMap["$group_by_labels"], actualMap["$group_by_labels"]) {
			t.Errorf("Group by labels do not match")
		}
		if !comparePrometheusQueries(expectedMap["$filter_labels"], actualMap["$filter_labels"]) {
			t.Errorf("Filterlabels do not match")
		}
	})

	t.Run("Mapping with empty fields", func(t *testing.T) {
		requestBody := MetricsRequestBody{
			MetricName: "test_metric",
		}
		expectedMap := map[string]string{
			"$metric_name":         "test_metric",
			"$filter_labels":       "",
			"$group_by_labels":     "",
			"$quantile_percentile": "",
			"$duration":            "",
		}

		promQlService := &PromQlService{}
		actualMap := promQlService.PopulateReqMap(requestBody)
		assert.Equal(t, actualMap["$metric_name"], expectedMap["$metric_name"])
		assert.Equal(t, actualMap["$quantile_percentile"], expectedMap["$quantile_percentile"])
		assert.Equal(t, actualMap["$duration"], expectedMap["$duration"])

		if !comparePrometheusQueries(expectedMap["$group_by_labels"], actualMap["$group_by_labels"]) {
			t.Errorf("Group by labels do not match")
		}
		if !comparePrometheusQueries(expectedMap["$filter_labels"], actualMap["$filter_labels"]) {
			t.Errorf("Filter labels do not match")
		}
	})
}
func Test_PromQueryBuilder(t *testing.T) {
	var service = &PromQlService{
		PlaceHolders: map[string][]string{
			"test_pattern": {"$quantile_percentile", "$group_by_labels", "$metric_name", "$filter_labels", "$duration"},
		},
		Expression: map[string]string{
			"test_pattern": "histogram_quantile($quantile_percentile, sum by($group_by_labels,le) (rate($metric_name{$filter_labels}[$duration])))",
		},
	}

	tests := []struct {
		name          string
		patternName   string
		requestBody   MetricsRequestBody
		expectedQuery string
		expectError   bool
	}{
		{
			name:        "Successful template substitution",
			patternName: "test_pattern",
			requestBody: MetricsRequestBody{
				PatternName:   "test_pattern",
				MetricName:    "test_bucket",
				Quantile:      "0.90",
				Duration:      "5m",
				GroupByLabels: []string{"pod"},
				FilterLabels: map[string]string{
					"namespace": "test_namespace",
					"mvtx_name": "test-mono-vertex",
					"pod":       "test-pod",
				},
			},
			expectedQuery: `histogram_quantile(0.90, sum by(pod,le) (rate(test_bucket{namespace= "test_namespace", mvtx_name= "test-mono-vertex", pod= "test-pod"}[5m])))`,
		},
		{
			name:        "Missing placeholder in req",
			patternName: "test_pattern",
			requestBody: MetricsRequestBody{
				PatternName:   "test_pattern",
				MetricName:    "test_bucket",
				Duration:      "5m",
				GroupByLabels: []string{"pod"},
				FilterLabels: map[string]string{
					"namespace": "test_namespace",
					"mvtx_name": "test-mono-vertex",
					"pod":       "test-pod",
				},
			},
			expectError: true,
		},
		{
			name:        "Missing pattern name in service config",
			patternName: "test_pattern_2",
			requestBody: MetricsRequestBody{
				PatternName:   "test_pattern_2",
				MetricName:    "test_bucket",
				Duration:      "5m",
				GroupByLabels: []string{"pod"},
				FilterLabels: map[string]string{
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
		expectedError := "prometheus client is nil"
		if err.Error() != expectedError {
			t.Errorf("expected error %v, got %v", expectedError, err)
		}
	})
}
