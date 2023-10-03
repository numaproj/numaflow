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

// Retry checking redis every 5 seconds.
const retryInterval = time.Second * 5

// RedisNotContains verifies that there is no occurrence of targetStr in redis that is written by pipelineName, sinkName.
func RedisNotContains(ctx context.Context, pipelineName, sinkName, targetStr string, opts ...SinkCheckOption) bool {
	o := defaultRedisCheckOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	return runChecks(ctx, func() bool {
		return !redisContains(pipelineName, sinkName, targetStr, 1)
	})
}

// RedisContains verifies that there are targetStr in redis written by pipelineName, sinkName.
func RedisContains(ctx context.Context, pipelineName, sinkName, targetStr string, opts ...SinkCheckOption) bool {
	o := defaultRedisCheckOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	return runChecks(ctx, func() bool {
		return redisContains(pipelineName, sinkName, targetStr, o.count)
	})
}

func redisContains(pipelineName, sinkName, targetStr string, expectedCount int) bool {
	// If number of matches is higher than expected, we treat it as passing the check.
	return GetMsgCountContains(pipelineName, sinkName, targetStr) >= expectedCount
}

type redisCheckOptions struct {
	count   int
	timeout time.Duration
}

func defaultRedisCheckOptions() *redisCheckOptions {
	return &redisCheckOptions{
		count:   1,
		timeout: defaultTimeout,
	}
}

type SinkCheckOption func(*redisCheckOptions)

// WithContainCount updates the redisCheckOptions to specify count.
// The count is the expected number of matches for the check.
func WithContainCount(c int) SinkCheckOption {
	return func(o *redisCheckOptions) {
		o.count = c
	}
}

// WithTimeout updates the redisCheckOptions to specify timeout.
// The timeout specifies how long the redis check will wait for expected data to be ready in redis.
func WithTimeout(t time.Duration) SinkCheckOption {
	return func(o *redisCheckOptions) {
		o.timeout = t
	}
}

type CheckFunc func() bool

// runChecks executes a performChecks function with retry strategy (retryInterval with timeout).
// If performChecks doesn't pass within timeout, runChecks returns false indicating the checks have failed.
// This is to mitigate the problem that we don't know exactly when a numaflow pipeline finishes processing our test data.
// Please notice such approach is not strictly accurate as there can be case where runChecks passes before pipeline finishes processing data.
// Which could result in false positive test results. e.g. checking data doesn't exist can pass before data gets persisted to redis.
func runChecks(ctx context.Context, performChecks CheckFunc) bool {
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Timeout reached, meaning checks did not pass within timeout, return false
			return false
		case <-ticker.C:
			if performChecks() {
				// All checks passed, return true
				return true
			}
		}
	}
}
