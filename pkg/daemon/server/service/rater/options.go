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

package server

type options struct {
	// Number of workers working on collecting counts of processed messages.
	workers int
	// Time in milliseconds, each element in the work queue will be picked up in an interval of this period of time.
	taskInterval int
}

type Option func(*options)

func defaultOptions() *options {
	// A simple example of how these numbers work together:
	// Assuming we have 200 tasks, we have 20 workers, each worker will be responsible for approximately 10 tasks during one iteration.
	// The task interval is 10 seconds, which means each task need to be picked up by a worker every 10 seconds.
	// Hence a worker needs to finish processing 1 task in 1 second.
	// Translating to numaflow language, for a 200-pod pipeline, a worker needs to finish scraping 1 pod in 1 second, which is a reasonable number.
	// TODO - need to adjust the numbers once we have a better idea of the performance
	return &options{
		workers: 20,
		// 10 seconds
		taskInterval: 10000,
	}
}

func WithWorkers(n int) Option {
	return func(o *options) {
		o.workers = n
	}
}

func WithTaskInterval(n int) Option {
	return func(o *options) {
		o.taskInterval = n
	}
}
