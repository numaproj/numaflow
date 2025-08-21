/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reconciler

import (
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// GlobalConfig is the configuration for the controllers, it is
// supposed to be populated from the configmap attached to the
// controller manager.
type GlobalConfig struct {
	conf *config
	lock *sync.RWMutex
}

type config struct {
	Instance string         `json:"instance"`
	Defaults *DefaultConfig `json:"defaults"`
	ISBSvc   *ISBSvcConfig  `json:"isbsvc"`
}

type DefaultConfig struct {
	ContainerResources string `json:"containerResources"`
}

type ISBSvcConfig struct {
	JetStream *JetStreamConfig `json:"jetstream"`
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

func (g *GlobalConfig) GetInstance() string {
	g.lock.RLock()
	defer g.lock.RUnlock()
	return g.conf.Instance
}

// Get controller scope default config
func (g *GlobalConfig) GetDefaults() DefaultConfig {
	g.lock.RLock()
	defer g.lock.RUnlock()
	if g.conf.Defaults != nil {
		return *g.conf.Defaults
	}
	return DefaultConfig{}
}

// Get controller scope ISB Service config
func (g *GlobalConfig) GetISBSvcConfig() ISBSvcConfig {
	g.lock.RLock()
	defer g.lock.RUnlock()
	if g.conf.ISBSvc != nil {
		return *g.conf.ISBSvc
	}
	return ISBSvcConfig{}
}

func (dc DefaultConfig) GetDefaultContainerResources() corev1.ResourceRequirements {
	// the standard resources used by the `init` and `main` containers.
	defaultResources := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{},
		Requests: corev1.ResourceList{
			"cpu":    resource.MustParse("100m"),
			"memory": resource.MustParse("128Mi"),
		},
	}

	if dc.ContainerResources == "" {
		return defaultResources
	}

	var resourceConfig corev1.ResourceRequirements
	err := yaml.Unmarshal([]byte(dc.ContainerResources), &resourceConfig)
	if err != nil {
		panic(fmt.Errorf("failed to unmarshal default container resources, %w", err))
	}

	return resourceConfig
}

func (isc ISBSvcConfig) GetJetStreamVersion(version string) (*JetStreamVersion, error) {
	if isc.JetStream == nil || len(isc.JetStream.Versions) == 0 {
		return nil, fmt.Errorf("no jetstream configuration found")
	}
	for _, r := range isc.JetStream.Versions {
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
	r := &GlobalConfig{
		lock: new(sync.RWMutex),
	}
	conf := &config{}
	err = v.Unmarshal(conf)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshal configuration file. %w", err)
	}
	r.conf = conf
	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		cf := &config{}
		err = v.Unmarshal(cf)
		if err != nil {
			onErrorReloading(err)
			return
		}
		r.lock.Lock()
		defer r.lock.Unlock()
		r.conf = cf
	})
	return r, nil
}
