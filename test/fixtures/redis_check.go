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

package fixtures

import (
	"context"
)

// RedisNotContains verifies that there is no key in redis which contain a substring matching the targetRegex.
func RedisNotContains(ctx context.Context, sinkName string, regex string) bool {
	return !redisContains(ctx, sinkName, regex, 1)
}

// RedisContains verifies that there are keys in redis which contain a substring matching the targetRegex.
func RedisContains(ctx context.Context, sinkName string, targetRegex string, opts ...RedisCheckOption) bool {
	o := defaultRedisCheckOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return redisContains(ctx, sinkName, targetRegex, o.count)

}

func redisContains(ctx context.Context, sinkName string, targetRegex string, expectedCount int) bool {
	// If number of matches is higher than expected, we treat it as passing the check.
	return GetMsgCountContains(sinkName, targetRegex) >= expectedCount
}

type redisCheckOptions struct {
	count int
}

func defaultRedisCheckOptions() *redisCheckOptions {
	return &redisCheckOptions{
		count: 1,
	}
}

type RedisCheckOption func(*redisCheckOptions)

// RedisCheckOptionWithCount updates the redisCheckOptions to specify count.
// The count is the expected number of matches for the check.
func RedisCheckOptionWithCount(c int) RedisCheckOption {
	return func(o *redisCheckOptions) {
		o.count = c
	}
}
