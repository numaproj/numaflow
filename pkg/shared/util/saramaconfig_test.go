package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSaramaConfigFromYAMLString(t *testing.T) {
	t.Run("YAML Config", func(t *testing.T) {
		var yamlExample = string(`
admin:
  retry:
    max: 103
producer:
  maxMessageBytes: 600
consumer:
  fetch: 
    min: 1
net:
  MaxOpenRequests: 5
`)
		conf, err := GetSaramaConfigFromYAMLString(yamlExample)
		assert.NoError(t, err)
		assert.Equal(t, 600, conf.Producer.MaxMessageBytes)
		assert.Equal(t, 103, conf.Admin.Retry.Max)
		assert.Equal(t, int32(1), conf.Consumer.Fetch.Min)
		assert.Equal(t, 5, conf.Net.MaxOpenRequests)
	})
	t.Run("Empty config", func(t *testing.T) {
		conf, err := GetSaramaConfigFromYAMLString("")
		assert.NoError(t, err)
		assert.Equal(t, 1000000, conf.Producer.MaxMessageBytes)
		assert.Equal(t, 5, conf.Admin.Retry.Max)
		assert.Equal(t, int32(1), conf.Consumer.Fetch.Min)
		assert.Equal(t, 5, conf.Net.MaxOpenRequests)
	})

	t.Run("NON yaml config", func(t *testing.T) {
		_, err := GetSaramaConfigFromYAMLString("welcome")
		assert.Error(t, err)

	})
}
