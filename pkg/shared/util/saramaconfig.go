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
