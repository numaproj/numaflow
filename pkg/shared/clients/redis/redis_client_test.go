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

package redis

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestNewRedisClient(t *testing.T) {
	// TODO: fix the test
	t.SkipNow()
	ctx := context.TODO()
	client := NewRedisClient(&redis.UniversalOptions{
		Addrs: []string{":6379"},
	})
	var stream = "foo"
	var streamGroup = "foo-group"
	err := client.CreateStreamGroup(ctx, stream, streamGroup, ReadFromEarliest)
	assert.NoError(t, err)
	defer func() {
		err := client.DeleteStreamGroup(ctx, stream, streamGroup)
		assert.NoError(t, err)
	}()

	err = client.CreateStreamGroup(ctx, stream, streamGroup, ReadFromEarliest)
	assert.Error(t, err)
}
