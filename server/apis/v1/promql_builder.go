package v1

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

type PromQl interface {
	QueryPrometheus(context.Context, string, time.Time, time.Time) (interface{}, error)
	BuildQuery(string, MetricsRequestBody) (string, error)
	PopulateReqMap(MetricsRequestBody) map[string]string
}

type PromQlBuilder struct {
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

// NewPromQlBuilder creates a new PromQlBuilder instance
func NewPromQlBuilder(client *Prometheus, config *PrometheusConfig) PromQl {
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

	return &PromQlBuilder{
		Prometheus:   client,
		PlaceHolders: placeHolders,
		Expression:   expressions,
		ConfigData:   config,
	}
}

// populate map based on req fields
func (b *PromQlBuilder) PopulateReqMap(requestBody MetricsRequestBody) map[string]string {
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
func (b *PromQlBuilder) BuildQuery(patternName string, requestBody MetricsRequestBody) (string, error) {
	var query string
	expr := b.Expression[patternName]
	placeHolders := b.PlaceHolders[patternName]
	reqMap := b.PopulateReqMap(requestBody)
	query, err := substitutePlaceHolders(expr, placeHolders, reqMap)
	if err != nil {
		return "", fmt.Errorf("error: %w", err)
	}
	return query, nil
}

// query prometheus server
func (b *PromQlBuilder) QueryPrometheus(ctx context.Context, promql string, start, end time.Time) (interface{}, error) {
	if b.Prometheus == nil {
		return nil, fmt.Errorf("prometheus client is nil")
	}
	r := v1.Range{
		Start: start,
		End:   end,
		Step:  time.Minute,
	}
	result, _, err := b.Prometheus.Api.QueryRange(ctx, promql, r)
	return result, err
}
