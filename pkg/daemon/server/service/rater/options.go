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

package rater

import "time"

type options struct {
	// Number of workers working on collecting counts of processed messages.
	workers int
	// Time in seconds, each element in the work queue will be picked up in an interval of this period of time.
	taskInterval time.Duration
}

type Option func(*options)

func defaultOptions() *options {
	// A simple example of how these numbers work together:
	// Assuming we have 200 tasks, we have 20 workers, each worker will be responsible for approximately 10 tasks during one iteration.
	// The task interval is 5 seconds, which means each task need to be picked up by a worker every 5 seconds.
	// Hence, a worker needs to finish processing 1 task in 0.5 second.
	// Translating to numaflow language, for a 200-pod pipeline, a worker needs to finish scraping 1 pod in 0.5 second, which is a reasonable number.
	return &options{
		workers: 50, // default max replicas is 50
		// we execute the rater metrics fetching every 5 seconds
		taskInterval: 5 * time.Second,
	}
}

func WithWorkers(n int) Option {
	return func(o *options) {
		o.workers = n
	}
}

func WithTaskInterval(duration time.Duration) Option {
	return func(o *options) {
		o.taskInterval = duration
	}
}
