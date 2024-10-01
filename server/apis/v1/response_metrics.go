package v1

import (
	"os"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"

	"gopkg.in/yaml.v2"
)

type PrometheusClient struct {
	// prometheus metric config from yaml
	ConfigData []map[string]any
	// prom client
	Client api.Client
	// prom client API to query data
	Api v1.API
}

type PrometheusConfig struct {
	// prometheus server url in the config
	ServerUrl string `yaml:"url"`
	// patterns in the config
	Patterns []map[string]any `yaml:"patterns"`
}

func NewPrometheusClient(config *PrometheusConfig) *PrometheusClient {
	if config == nil || config.ServerUrl == "" {
		return nil
	}
	client, err := api.NewClient(api.Config{
		Address: config.ServerUrl,
	})
	if err != nil {
		return nil
	}
	v1api := v1.NewAPI(client)
	return &PrometheusClient{
		ConfigData: config.Patterns,
		Client:     client,
		Api:        v1api,
	}
}

func loadPrometheusMetricConfig() *PrometheusConfig {
	var (
		data       []byte
		promConfig PrometheusConfig
		err        error
	)

	// read prometheus metric config yaml from volume mount path
	data, err = os.ReadFile("/etc/numaflow/metrics/config.yaml")
	if err != nil {
		return nil
	}
	err = yaml.Unmarshal(data, &promConfig)

	if err != nil {
		return nil
	}

	return &promConfig
}
