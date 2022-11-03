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

package util

import (
	"bytes"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

// GetSaramaConfigFromYAMLString parse yaml string to sarama.config
func GetSaramaConfigFromYAMLString(yaml string) (*sarama.Config, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewBufferString(yaml)); err != nil {
		return nil, err
	}
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unable to decode into struct, %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("failed validating sarama config, %w", err)
	}
	return cfg, nil
}
