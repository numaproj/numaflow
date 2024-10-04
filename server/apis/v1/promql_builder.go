package v1

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

type PromQlBuilder struct {
	PlaceHolders map[string]string
	Expression   string
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

// throws err if any required placeholder in expr is not present in req body
func substitutePlaceHolders(expr string, placeholders map[string]string) (string, error) {
	re := regexp.MustCompile(`\$(\w+)`)
	matches := re.FindAllStringSubmatch(expr, -1)

	for _, match := range matches {
		key := match[0]
		val, ok := placeholders[key]
		if !ok || val == "" {
			return "", fmt.Errorf("req body doesn't have %s field", match[1])
		}
		expr = strings.Replace(expr, key, val, -1)
	}
	return expr, nil
}

// NewPromQlBuilder creates a new PromQlBuilder instance
func NewPromQlBuilder() *PromQlBuilder {
	return &PromQlBuilder{
		PlaceHolders: make(map[string]string),
		Expression:   "",
	}
}

var builderPool = sync.Pool{
	New: func() interface{} {
		return NewPromQlBuilder()
	},
}

func getBuilder() *PromQlBuilder {
	return builderPool.Get().(*PromQlBuilder)
}

func putBuilder(b *PromQlBuilder) {
	b.Expression = ""
	b.PlaceHolders = make(map[string]string)
	builderPool.Put(b)
}

// populate placeholders and expression with req body and pattern expr
func (b *PromQlBuilder) PopulateBuilder(requestBody MetricsRequestBody, expr string) *PromQlBuilder {
	b.Expression = expr
	b.PlaceHolders = map[string]string{
		"$metric_name":         requestBody.MetricName,
		"$filter_labels":       formatMapLabels(requestBody.FilterLabels),
		"$group_by_labels":     formatArrayLabels(requestBody.GroupByLabels),
		"$quantile_percentile": requestBody.Quantile,
		"$duration":            requestBody.Duration,
	}
	return b
}

// Build constructs the PromQL query string
func (b *PromQlBuilder) Build() (string, error) {
	var query string
	if b.Expression == "" {
		return "", fmt.Errorf("expr not set for the pattern")
	}
	query, err := substitutePlaceHolders(b.Expression, b.PlaceHolders)
	if err != nil {
		return "", fmt.Errorf("error: %w", err)
	}
	return query, nil
}
