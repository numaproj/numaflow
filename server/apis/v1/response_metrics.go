package v1

import (
	"fmt"
	"os"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"

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
	Expression  *string      `yaml:"expr"`
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

type PrometheusClientInterface interface {
	GetClientAndApi() (api.Client, v1.API, error)
	GetConfigData() []PatternData
}

type PrometheusClient struct {
	ConfigData []PatternData
	Client     api.Client
	Api        v1.API
}

func NewPrometheusClient(config *PrometheusConfig) PrometheusClientInterface {
	if config == nil || config.ServerUrl == "" {
		return &PrometheusClient{}
	}
	client, err := api.NewClient(api.Config{
		Address: config.ServerUrl,
	})
	if err != nil {
		return &PrometheusClient{}
	}
	v1api := v1.NewAPI(client)
	return &PrometheusClient{
		ConfigData: config.Patterns,
		Client:     client,
		Api:        v1api,
	}
}

func (pc *PrometheusClient) GetClientAndApi() (api.Client, v1.API, error) {
	if pc != nil && pc.Client != nil && pc.Api != nil {
		return pc.Client, pc.Api, nil
	}
	return nil, nil, fmt.Errorf("prometheus client/api not set")
}

func (pc *PrometheusClient) GetConfigData() []PatternData {
	if pc != nil {
		return pc.ConfigData
	}
	return []PatternData{}
}

func loadPrometheusMetricConfig() *PrometheusConfig {
	var (
		data       []byte
		promConfig PrometheusConfig
		err        error
	)

	data, err = os.ReadFile(metricsProxyConfigPath)
	if err != nil {
		return nil
	}
	err = yaml.Unmarshal(data, &promConfig)

	if err != nil {
		return nil
	}

	return &promConfig
}
