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
	"time"
)

const timeout = time.Minute * 1
const retryInterval = time.Second * 5

// RedisNotContains verifies that there is no key in redis which contain a substring matching the targetRegex.
func RedisNotContains(ctx context.Context, sinkName string, regex string) bool {
	return runChecks(func() bool {
		return !redisContains(ctx, sinkName, regex, 1)
	})
}

// RedisContains verifies that there are keys in redis which contain a substring matching the targetRegex.
func RedisContains(ctx context.Context, sinkName string, targetRegex string, opts ...RedisCheckOption) bool {
	o := defaultRedisCheckOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}

	return runChecks(func() bool {
		return redisContains(ctx, sinkName, targetRegex, o.count)
	})
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

type CheckFunc func() bool

// runChecks executes a performChecks function with retry strategy (retryInterval with timeout).
// If performChecks doesn't pass within timeout, runChecks returns false indicating the checks have failed.
// This is to mitigate the problem that we don't know exactly when a numaflow pipeline finishes processing our test data.
// Please notice such approach is not strictly accurate as there can be case where runChecks passes before pipeline finishes processing data.
// Which could result in false positive test results. e.g. checking data doesn't exist can pass before data gets persisted to redis.
func runChecks(performChecks CheckFunc) bool {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		if performChecks() {
			// All checks passed, so return true
			return true
		}

		if time.Now().After(deadline) {
			// Timeout reached, so return false
			return false
		}

		// Wait for the next tick of the ticker
		<-ticker.C
	}
}
