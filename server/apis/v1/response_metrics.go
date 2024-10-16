package v1

import (
	"os"

	"gopkg.in/yaml.v2"
)

const (
	metricsProxyConfigPath = "/etc/numaflow/metrics/config.yaml"
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

type FilterData struct {
	Name     string `yaml:"name"`
	Required bool   `yaml:"required"`
}

type DimensionData struct {
	Name       string       `yaml:"name"`
	Expression string       `yaml:"expr"`
	Filters    []FilterData `yaml:"filters"`
}

type MetricData struct {
	Name string `yaml:"metric_name"`
	// array of required labels.
	Filters []string `yaml:"required_filters"`
	//array of dimensions and their data
	Dimensions []DimensionData `yaml:"dimensions"`
}

type ParamData struct {
	Name     string `yaml:"name"`
	Required bool   `yaml:"required"`
}
type PatternData struct {
	Name        string       `yaml:"name" json:"name"`
	Object      string       `yaml:"object" json:"object"`
	Title       string       `yaml:"title"`
	Description string       `yaml:"description"`
	Expression  string       `yaml:"expr"`
	Params      []ParamData  `yaml:"params"`
	Metrics     []MetricData `yaml:"metrics"`
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
