package v1

import (
	"os"

	"gopkg.in/yaml.v2"
)

const (
	metricsProxyConfigPath = "/etc/numaflow/metrics/config.yaml"
)

type MetricsRequestBody struct {
	PatternName string `json:"pattern_name"`
	// required because a pattern can have multiple metric_names
	MetricName    string            `json:"metric_name"`
	FilterLabels  map[string]string `json:"filter_labels"`
	GroupByLabels []string          `json:"group_by_labels"`
	Duration      string            `json:"duration"`
	Quantile      string            `json:"quantile_percentile"`
	StartTime     string            `json:"start_time"`
	EndTime       string            `json:"end_time"`
}

type MetricData struct {
	Name string `yaml:"metric_name"`
	// array of supported filter labels.
	FilterLabels []string `yaml:"filter_labels"`
	//array of supported group by labels
	GroupByLabels []string `yaml:"group_by_labels"`
}
type PatternData struct {
	Name        string       `yaml:"name" json:"name"`
	Object      string       `yaml:"object" json:"object"`
	Title       string       `yaml:"title"`
	Description string       `yaml:"description"`
	Expression  string       `yaml:"expr"`
	Duration    []string     `yaml:"duration"`
	Metrics     []MetricData `yaml:"metrics"`
	Quantile    []string     `yaml:"quantile_percentile"`
}

type PrometheusConfig struct {
	// prometheus server url in the config
	ServerUrl string `yaml:"url"`
	// patterns in the config
	Patterns []PatternData `yaml:"patterns"`
}

func loadPrometheusMetricConfig() *PrometheusConfig {
	var (
		data       []byte
		promConfig PrometheusConfig
		err        error
	)

	data, err = os.ReadFile(metricsProxyConfigPath)
	if err != nil {
		return &PrometheusConfig{}
	}
	err = yaml.Unmarshal(data, &promConfig)

	if err != nil {
		return &PrometheusConfig{}
	}
	return &promConfig
}
