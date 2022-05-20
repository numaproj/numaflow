package controllers

import (
	"fmt"

	"github.com/fsnotify/fsnotify"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/spf13/viper"
)

type GlobalConfig struct {
	UDF    *UDFConfig    `json:"udf"`
	Sink   *SinkConfig   `json:"sink"`
	ISBSvc *ISBSvcConfig `json:"isbsvc"`
}

type UDFConfig struct {
	ContentType string `json:"contentType"`
}

type SinkConfig struct {
	ContentType string `json:"contentType"`
}

type ISBSvcConfig struct {
	Redis     *RedisConfig     `json:"redis"`
	JetStream *JetStreamConfig `json:"jetstream"`
}

type RedisConfig struct {
	Settings *RedisSettings `json:"settings"`
	Versions []RedisVersion `json:"versions"`
}

type RedisSettings struct {
	Redis    string `json:"redis"`
	Master   string `json:"master"`
	Replica  string `json:"replica"`
	Sentinel string `json:"sentinel"`
}

type RedisVersion struct {
	Version            string `json:"version"`
	RedisImage         string `json:"redisImage"`
	SentinelImage      string `json:"sentinelImage"`
	InitContainerImage string `json:"initContainerImage"`
	RedisExporterImage string `json:"redisExporterImage"`
}

type JetStreamConfig struct {
	Settings     string             `json:"settings"`
	BufferConfig string             `json:"bufferConfig"`
	Versions     []JetStreamVersion `json:"versions"`
}

type JetStreamVersion struct {
	Version              string `json:"version"`
	NatsImage            string `json:"natsImage"`
	MetricsExporterImage string `json:"metricsExporterImage"`
	ConfigReloaderImage  string `json:"configReloaderImage"`
	StartCommand         string `json:"startCommand"`
}

func (g *GlobalConfig) GetUDFContentType() dfv1.ContentType {
	if g.UDF == nil || g.UDF.ContentType == "" {
		// Defaults to application/msgpack
		return dfv1.MsgPackType
	}
	switch dfv1.ContentType(g.UDF.ContentType) {
	case dfv1.JsonType:
		return dfv1.JsonType
	case dfv1.MsgPackType:
		return dfv1.MsgPackType
	default:
		panic(fmt.Sprintf("Unsupported UDF encoding %q", g.UDF.ContentType))
	}
}

func (g *GlobalConfig) GetUDSinkContentType() dfv1.ContentType {
	if g.Sink == nil || g.Sink.ContentType == "" {
		// Defaults to application/msgpack
		return dfv1.MsgPackType
	}
	switch dfv1.ContentType(g.Sink.ContentType) {
	case dfv1.JsonType:
		return dfv1.JsonType
	case dfv1.MsgPackType:
		return dfv1.MsgPackType
	default:
		panic(fmt.Sprintf("Unsupported UDSink encoding %q", g.UDF.ContentType))
	}
}

func (g *GlobalConfig) GetRedisVersion(version string) (*RedisVersion, error) {
	if g.ISBSvc == nil || g.ISBSvc.Redis == nil || len(g.ISBSvc.Redis.Versions) == 0 {
		return nil, fmt.Errorf("no redis configuration found")
	}
	for _, r := range g.ISBSvc.Redis.Versions {
		if r.Version == version {
			return &r, nil
		}
	}
	return nil, fmt.Errorf("no redis configuration found for %q", version)
}

func (g *GlobalConfig) GetJetStreamVersion(version string) (*JetStreamVersion, error) {
	if g.ISBSvc.JetStream == nil || len(g.ISBSvc.JetStream.Versions) == 0 {
		return nil, fmt.Errorf("no jetstream configuration found")
	}
	for _, r := range g.ISBSvc.JetStream.Versions {
		if r.Version == version {
			return &r, nil
		}
	}
	return nil, fmt.Errorf("no jetstream configuration found for %q", version)
}

func LoadConfig(onErrorReloading func(error)) (*GlobalConfig, error) {
	v := viper.New()
	v.SetConfigName("controller-config")
	v.SetConfigType("yaml")
	v.AddConfigPath("/etc/numaflow")
	err := v.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration file. %w", err)
	}
	r := &GlobalConfig{}
	err = v.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshal configuration file. %w", err)
	}
	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		err = v.Unmarshal(r)
		if err != nil {
			onErrorReloading(err)
		}
	})
	return r, nil
}
