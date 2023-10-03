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
