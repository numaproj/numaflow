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

package scaling

type options struct {
	// Number of workers working on autoscaling.
	workers int
	// Time in milliseconds, each element in the work queue will be picked up in an interval of this period of time.
	taskInterval int
	// Threshold of considering there's back pressure, a float value less than 1.
	backPressureThreshold float64
	// size of the daemon clients cache.
	clientsCacheSize int
}

type Option func(*options)

func defaultOptions() *options {
	return &options{
		workers:               20,
		taskInterval:          30000,
		backPressureThreshold: 0.9,
		clientsCacheSize:      500,
	}
}

// WithWorkers sets the number of workers working on autoscaling.
func WithWorkers(n int) Option {
	return func(o *options) {
		o.workers = n
	}
}

// WithTaskInterval sets the interval of picking up a task from the work queue.
func WithTaskInterval(n int) Option {
	return func(o *options) {
		o.taskInterval = n
	}
}

// WithBackPressureThreshold sets the threshold of considering there's back pressure, a float value less than 1.
func WithBackPressureThreshold(n float64) Option {
	return func(o *options) {
		o.backPressureThreshold = n
	}
}

// WithClientsCacheSize sets the size of the daemon clients cache.
func WithClientsCacheSize(n int) Option {
	return func(o *options) {
		o.clientsCacheSize = n
	}
}
