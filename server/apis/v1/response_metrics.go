package v1

import (
	"os"

	"gopkg.in/yaml.v2"
)

const (
	metricsProxyConfigPath = "/etc/numaflow/metrics-proxy/config.yaml"
)

type MetricsRequestBody struct {
	MetricName string            `json:"metric_name"`
	Dimension  string            `json:"dimension"`
	Filters    map[string]string `json:"filters"`
	Duration   string            `json:"duration"`
	Quantile   string            `json:"quantile"`
	StartTime  string            `json:"start_time"`
	EndTime    string            `json:"end_time"`
}

type Filter struct {
	Name     string `yaml:"name"`
	Required bool   `yaml:"required"`
}

type Dimension struct {
	Name       string   `yaml:"name"`
	Expression string   `yaml:"expr"`
	Filters    []Filter `yaml:"filters"`
}

type Metric struct {
	Name        string `yaml:"metric_name"`
	DisplayName string `yaml:"display_name"`
	Unit        string `yaml:"unit"`
	// array of required labels.
	Filters []string `yaml:"required_filters"`
	//array of dimensions and their data
	Dimensions []Dimension `yaml:"dimensions"`
}

type Params struct {
	Name     string `yaml:"name"`
	Required bool   `yaml:"required"`
}

type Pattern struct {
	Name        string   `yaml:"name" json:"name"`
	Object      string   `yaml:"object" json:"object"`
	Title       string   `yaml:"title"`
	Description string   `yaml:"description"`
	Expression  string   `yaml:"expr"`
	Params      []Params `yaml:"params"`
	Metrics     []Metric `yaml:"metrics"`
}

type PrometheusConfig struct {
	// prometheus server url in the config
	ServerUrl string `yaml:"url"`
	// patterns in the config
	Patterns []Pattern `yaml:"patterns"`
}

var loadPrometheusMetricConfig = func() (*PrometheusConfig, error) {
	var (
		data       []byte
		promConfig PrometheusConfig
		err        error
	)

	data, err = os.ReadFile(metricsProxyConfigPath)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(data, &promConfig)

	if err != nil {
		return nil, err
	}
	return &promConfig, nil
}
