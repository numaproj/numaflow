package v1

import (
	"fmt"
	"strings"
	"sync"
)

type PromQlBuilder struct {
	metricName     string
	labels         map[string]string
	rangeVector    string
	aggregator     string
	groubByFilters []string
	innerFunction  string
	function       string
}

// NewPromQlBuilder creates a new PromQlBuilder instance
func NewPromQlBuilder() *PromQlBuilder {
	return &PromQlBuilder{
		labels:         make(map[string]string),
		groubByFilters: make([]string, 0),
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
	b.metricName = "" // Reset the builder
	b.innerFunction = ""
	b.labels = make(map[string]string)
	b.rangeVector = ""
	b.aggregator = ""
	b.groubByFilters = make([]string, 0)
	b.function = ""
	builderPool.Put(b)
}

func (b *PromQlBuilder) WithMetricName(name string) *PromQlBuilder {
	b.metricName = name
	return b
}

// WithLabels adds  labels to the PromQL query
func (b *PromQlBuilder) WithLabels(key, value string) *PromQlBuilder {
	b.labels[key] = value
	return b
}

// WithRangeVector adds a range vector to the PromQL query
func (b *PromQlBuilder) WithRangeVector(rangeVector string) *PromQlBuilder {
	b.rangeVector = rangeVector
	return b
}

// WithAggregator adds an aggregator to the PromQL query
func (b *PromQlBuilder) WithAggregator(aggregator string) *PromQlBuilder {
	if aggregator == "" {
		return b
	}
	b.aggregator = aggregator
	return b
}

// WithGroupByFilter adds group by filters to the PromQL query
func (b *PromQlBuilder) WithGroupByFilters(filters []string) *PromQlBuilder {
	if len(filters) == 0 {
		return b
	}
	b.groubByFilters = make([]string, 0)
	b.groubByFilters = append(b.groubByFilters, filters...)
	return b
}

// Add Inner functions like "rate"
func (b *PromQlBuilder) WithInnerFunction(function string) *PromQlBuilder {
	if function == "" {
		return b
	}
	var innerFnStr strings.Builder
	innerFnStr.WriteString(function)
	b.innerFunction = innerFnStr.String()
	return b
}

// WithFunction adds a function to the PromQL query like histogram_quantile with args like quantiles(0.99)
func (b *PromQlBuilder) WithFunction(functionName string, args ...string) *PromQlBuilder {
	if functionName == "" {
		return b
	}
	var argsStr strings.Builder
	for i, arg := range args {
		if i > 0 {
			argsStr.WriteString(", ")
		}
		argsStr.WriteString(arg)
	}
	b.function = fmt.Sprintf("%s(%s,", functionName, argsStr.String())
	return b
}

func (b *PromQlBuilder) PopulateBuilder(requestBody MetricsRequestBody) *PromQlBuilder {
	b.WithMetricName(requestBody.MetricName)
	for k, v := range requestBody.Labels {
		b.WithLabels(k, v)
	}
	b.WithInnerFunction(requestBody.InnerFunction)
	b.WithRangeVector(requestBody.RangeVector)
	b.WithAggregator(requestBody.Aggregator)
	b.WithGroupByFilters(requestBody.GroupByLabels)
	b.WithFunction(requestBody.Function.Name, requestBody.Function.Args...)
	return b
}

/*
Build constructs the PromQL query string

# All below queries are valid queries

1. query: metric name + labels
2. inner_query: inner_function(eg: rate) + query(in point 1) + range vector
3. outer_query w/o aggregator: function + args + 2/1
4. outer_query with aggregator: function + args + aggregator query + 2/1
*/

func (b *PromQlBuilder) Build() string {

	var query strings.Builder
	query.WriteString(b.metricName)
	if len(b.labels) > 0 {
		query.WriteString("{")
		firstLabel := true
		for k, v := range b.labels {
			if !firstLabel {
				query.WriteString(",")
			}
			query.WriteString(k)
			query.WriteString("=\"")
			query.WriteString(v)
			query.WriteString("\"")
			firstLabel = false
		}
		query.WriteString("}")
	}

	var innerQuery strings.Builder
	if b.innerFunction != "" && b.rangeVector != "" {
		innerQuery.WriteString(b.innerFunction)
		innerQuery.WriteString("(" + query.String() + "[" + b.rangeVector + "]" + ")")
	} else {
		innerQuery.WriteString(query.String())
	}

	var aggregatorQuery strings.Builder
	if b.aggregator != "" && len(b.groubByFilters) > 0 {
		aggregatorQuery.WriteString(b.aggregator)
		aggregatorQuery.WriteString(" by(")
		for _, filter := range b.groubByFilters {
			aggregatorQuery.WriteString(filter + ", ")
		}
		aggregatorQuery.WriteString("le) ")
	}

	var outerQuery strings.Builder
	if b.function != "" {
		outerQuery.WriteString(b.function)
		outerQuery.WriteString(aggregatorQuery.String())
	}
	if outerQuery.Len() > 0 {
		outerQuery.WriteString("(")
		outerQuery.WriteString(innerQuery.String())
		outerQuery.WriteString("))")

	} else {
		outerQuery.WriteString(innerQuery.String())
	}

	return outerQuery.String()
}
