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
	// implement client methods here
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

func NewPrometheusClient(url string) *Prometheus {
	if url == "" {
		return nil
	}
	client, err := api.NewClient(api.Config{
		Address: url,
	})
	if err != nil {
		return nil
	}
	v1api := v1.NewAPI(client)
	return &Prometheus{
		Client: client,
		Api:    v1api,
	}
}

type PromQl interface {
	QueryPrometheus(context.Context, string, time.Time, time.Time) (model.Value, error)
	BuildQuery(MetricsRequestBody) (string, error)
	PopulateReqMap(MetricsRequestBody) map[string]string
}

type PromQlService struct {
	Prometheus   *Prometheus
	PlaceHolders map[string][]string
	Expression   map[string]string
	ConfigData   *PrometheusConfig
}

func formatArrayLabels(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	if len(labels) == 1 {
		return labels[0]
	}
	var builder strings.Builder
	first := true

	for _, v := range labels {
		builder.WriteString(v)
		if first {
			builder.WriteString(", ")
		}
		first = false
	}
	return builder.String()
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

// substitues placeholders in expr with req values
// throws err if any required placeholder is not present/empty in reqmap
func substitutePlaceHolders(expr string, placeholders []string, reqMap map[string]string) (string, error) {
	for _, match := range placeholders {
		key := match
		val, ok := reqMap[key]
		if !ok || val == "" {
			return "", fmt.Errorf("req body doesn't have %s field", key)
		}
		expr = strings.Replace(expr, key, val, -1)
	}
	return expr, nil
}

// NewPromQlService creates a new PromQlService instance
func NewPromQlService(client *Prometheus, config *PrometheusConfig) PromQl {
	var (
		expressions  = make(map[string]string)
		placeHolders = make(map[string][]string)
	)
	for _, pattern := range config.Patterns {
		name := pattern.Name
		expr := pattern.Expression
		placeHoldersArr := make([]string, 0)
		re := regexp.MustCompile(`\$(\w+)`)
		matches := re.FindAllStringSubmatch(expr, -1)
		for _, match := range matches {
			placeHoldersArr = append(placeHoldersArr, match[0])
		}
		expressions[name] = expr
		placeHolders[name] = placeHoldersArr
	}

	return &PromQlService{
		Prometheus:   client,
		PlaceHolders: placeHolders,
		Expression:   expressions,
		ConfigData:   config,
	}
}

// populate map based on req fields
func (b *PromQlService) PopulateReqMap(requestBody MetricsRequestBody) map[string]string {
	reqMap := map[string]string{
		"$metric_name":         requestBody.MetricName,
		"$filter_labels":       formatMapLabels(requestBody.FilterLabels),
		"$group_by_labels":     formatArrayLabels(requestBody.GroupByLabels),
		"$quantile_percentile": requestBody.Quantile,
		"$duration":            requestBody.Duration,
	}
	return reqMap
}

// build constructs the PromQL query string
func (b *PromQlService) BuildQuery(requestBody MetricsRequestBody) (string, error) {
	var query string
	var patternName = requestBody.PatternName
	expr := b.Expression[patternName]
	placeHolders := b.PlaceHolders[patternName]

	if expr == "" || len(placeHolders) == 0 {
		return query, fmt.Errorf("expr or placeholders do not exist for the pattern in the config")
	}
	reqMap := b.PopulateReqMap(requestBody)
	query, err := substitutePlaceHolders(expr, placeHolders, reqMap)
	if err != nil {
		return "", fmt.Errorf("error: %w", err)
	}
	return query, nil
}

// query prometheus server
func (b *PromQlService) QueryPrometheus(ctx context.Context, promql string, start, end time.Time) (model.Value, error) {
	if b.Prometheus == nil {
		return nil, fmt.Errorf("prometheus client is nil")
	}
	r := v1.Range{
		Start: start,
		End:   end,
		Step:  time.Minute,
	}
	result, _, err := b.Prometheus.Api.QueryRange(ctx, promql, r, v1.WithTimeout(5*time.Second))
	return result, err
}
