package v1

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// PrometheusClient interface for the Prometheus HTTP client
type PrometheusClient interface {
	// Do implement client methods here
	Do(context.Context, *http.Request) (*http.Response, []byte, error)
}

// PrometheusAPI interface for the Prometheus API
type PrometheusAPI interface {
	QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error)
}

// Prometheus struct holds the client and API
type Prometheus struct {
	Client PrometheusClient
	Api    PrometheusAPI
}

func NewPrometheusClient(url string) (*Prometheus, error) {
	if url == "" {
		return nil, fmt.Errorf("prometheus server url is not set")
	}
	client, err := api.NewClient(api.Config{
		Address: url,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus client, %w", err)
	}
	v1api := v1.NewAPI(client)
	return &Prometheus{
		Client: client,
		Api:    v1api,
	}, nil
}

type PromQl interface {
	QueryPrometheus(context.Context, string, time.Time, time.Time) (model.Value, error)
	BuildQuery(MetricsRequestBody) (string, error)
	PopulateReqMap(MetricsRequestBody) map[string]string
	GetConfigData() *PrometheusConfig
	DisableMetricsChart() bool
}

type PromQlService struct {
	PrometheusClient *Prometheus
	PlaceHolders     map[string]map[string][]string
	Expression       map[string]map[string]string
	ConfigData       *PrometheusConfig
}

func formatDimension(dimension string) string {
	switch dimension {
	case "mono-vertex":
		return "mvtx_name"
	default:
		return dimension
	}
}

// builds key, val pair string for labels
func formatMapLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	var builder strings.Builder
	first := true

	for k, v := range labels {
		if !first {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("%s= \"%s\"", k, v))
		first = false
	}
	return builder.String()
}

// substitutes placeholders in expr with req values
// throws err if any required placeholder is not present/empty in reqMap
func substitutePlaceHolders(expr string, placeholders []string, reqMap map[string]string) (string, error) {
	for _, match := range placeholders {
		key := match
		val, ok := reqMap[key]
		if !ok || val == "" {
			return "", fmt.Errorf("missing the %s field in the request body", key)
		}
		expr = strings.Replace(expr, key, val, -1)
	}
	return expr, nil
}

// NewPromQlServiceObject creates a new PromQlService instance
func NewPromQlServiceObject() (PromQl, error) {
	var (
		// map of [metric_name][dimension] = expr
		expressions  = make(map[string]map[string]string)
		placeHolders = make(map[string]map[string][]string)
		client       *Prometheus
		config       *PrometheusConfig
		err          error
	)

	var serviceObj = &PromQlService{
		PrometheusClient: client,
		PlaceHolders:     placeHolders,
		Expression:       expressions,
		ConfigData:       config,
	}

	// load prometheus metric config.
	config, err = LoadPrometheusMetricConfig()
	if err != nil {
		// return serviceObj with nil config data & client. Do not return error as this is not critical.
		return serviceObj, nil
	}
	serviceObj.ConfigData = config

	// prometheus client instance.
	client, err = NewPrometheusClient(config.ServerUrl)
	if err != nil {
		// return serviceObj with nil prometheus client. Do not return error as this is not critical.
		return serviceObj, nil
	}
	serviceObj.PrometheusClient = client

	for _, pattern := range config.Patterns {
		patternExpression := pattern.Expression
		for _, metric := range pattern.Metrics {
			metricName := metric.Name
			for _, dimension := range metric.Dimensions {
				dimensionName := dimension.Name
				_, ok := expressions[metricName]
				if !ok {
					expressions[metricName] = make(map[string]string)
				}
				if dimension.Expression != "" {
					expressions[metricName][dimensionName] = dimension.Expression
				} else {
					expressions[metricName][dimensionName] = patternExpression
				}
				expr := expressions[metricName][dimensionName]
				placeHoldersArr := make([]string, 0)
				re := regexp.MustCompile(`\$(\w+)`)
				matches := re.FindAllStringSubmatch(expr, -1)
				for _, match := range matches {
					placeHoldersArr = append(placeHoldersArr, match[0])
				}
				_, ok = placeHolders[metricName]
				if !ok {
					placeHolders[metricName] = map[string][]string{}
				}
				placeHolders[metricName][dimensionName] = placeHoldersArr
			}
		}
	}

	serviceObj.PlaceHolders = placeHolders
	serviceObj.Expression = expressions
	return serviceObj, nil
}

// PopulateReqMap populate map based on req fields
func (b *PromQlService) PopulateReqMap(requestBody MetricsRequestBody) map[string]string {
	reqMap := map[string]string{
		"$metric_name": requestBody.MetricName,
		"$filters":     formatMapLabels(requestBody.Filters),
		"$dimension":   formatDimension(requestBody.Dimension),
		"$quantile":    requestBody.Quantile,
		"$duration":    requestBody.Duration,
	}
	return reqMap
}

// BuildQuery build constructs the PromQL query string
func (b *PromQlService) BuildQuery(requestBody MetricsRequestBody) (string, error) {
	var query string
	var metricName = requestBody.MetricName
	var dimension = requestBody.Dimension
	if metricName == "" || dimension == "" {
		return query, fmt.Errorf("missing metric name or dimension in the request body")
	}
	expr, ok := b.Expression[metricName][dimension]
	if !ok {
		return query, fmt.Errorf(`expression is not defined for "%s" dimension of "%s" metric`, dimension, metricName)
	}
	placeHolders, ok := b.PlaceHolders[metricName][dimension]
	if !ok {
		return query, fmt.Errorf(`placeholders are not defined for "%s" dimension of "%s" metric`, dimension, metricName)
	}

	if expr == "" || len(placeHolders) == 0 {
		return query, fmt.Errorf(`expression or placeholders do not exist for for "%s" dimension of "%s" metric in the metrics config`, dimension, metricName)
	}
	reqMap := b.PopulateReqMap(requestBody)
	query, err := substitutePlaceHolders(expr, placeHolders, reqMap)
	if err != nil {
		return "", fmt.Errorf("failed to substitute placeholders: %w", err)
	}
	return query, nil
}

// QueryPrometheus query prometheus server
func (b *PromQlService) QueryPrometheus(ctx context.Context, promql string, start, end time.Time) (model.Value, error) {
	if b.PrometheusClient == nil {
		return nil, fmt.Errorf("prometheus client is not defined")
	}
	r := v1.Range{
		Start: start,
		End:   end,
		Step:  time.Minute,
	}
	result, _, err := b.PrometheusClient.Api.QueryRange(ctx, promql, r, v1.WithTimeout(5*time.Second))
	return result, err
}

// GetConfigData returns the PrometheusConfig
func (b *PromQlService) GetConfigData() *PrometheusConfig {
	return b.ConfigData
}

func (b *PromQlService) DisableMetricsChart() bool {
	// disable metrics charts if metric config or prometheus client is nil
	return b.ConfigData == nil || b.PrometheusClient == nil
}
