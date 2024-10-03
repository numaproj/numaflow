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

type FunctionStruct struct {
	Name string   `json:"name" yaml:"name"`
	Args []string `json:"args" yaml:"args"`
}
type MetricsRequestBody struct {
	Name          string            `json:"name"`
	MetricName    string            `json:"metric_name"`
	Labels        map[string]string `json:"labels"`
	GroupByLabels []string          `json:"group_by_labels"`
	Aggregator    string            `json:"aggregator"`
	RangeVector   string            `json:"range_vector"`
	StartTime     string            `json:"start_time"`
	EndTime       string            `json:"end_time"`
	InnerFunction string            `json:"inner_function"`
	Function      FunctionStruct    `json:"function"`
}

// To Do: Dynamically get labels for a metric from server
type MetricData struct {
	Name string `yaml:"metric_name"`
	// array of supported filter labels.
	FilterLabels []string `yaml:"filter_labels"`
	//array of supported group by labels
	GroupByLabels []string `yaml:"group_by_labels"`
}
type PatternData struct {
	Name          string         `yaml:"name" json:"name"`
	Object        string         `yaml:"object" json:"object"`
	Title         string         `yaml:"title"`
	Description   string         `yaml:"description"`
	Expression    *string        `yaml:"expr"`
	RangeVector   string         `yaml:"rangeVector"`
	Metrics       []MetricData   `yaml:"metrics"`
	InnerFunction string         `yaml:"inner_function"`
	Function      FunctionStruct `yaml:"function"`
}

type PrometheusConfig struct {
	// prometheus server url in the config
	ServerUrl string `yaml:"url"`
	// patterns in the config
	Patterns []PatternData `yaml:"patterns"`
}

type MetricServerClientInterface interface {
	GetClient() (api.Client, error)
	GetClientApi() (v1.API, error)
	GetConfigData() []PatternData
}

type PrometheusClient struct {
	// prometheus metric config from yaml
	ConfigData []PatternData
	// prom client
	Client api.Client
	// prom client API to query data
	Api v1.API
}

func NewPrometheusClient(config *PrometheusConfig) MetricServerClientInterface {
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

func (pc *PrometheusClient) GetClient() (api.Client, error) {
	if pc != nil && pc.Client != nil {
		return pc.Client, nil
	}
	return nil, fmt.Errorf("prometheus client not set")
}

func (pc *PrometheusClient) GetClientApi() (v1.API, error) {
	if pc != nil && pc.Api != nil {
		return pc.Api, nil
	}
	return nil, fmt.Errorf("prometheus api not set")
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

	// read prometheus metric config yaml from volume mount path
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
