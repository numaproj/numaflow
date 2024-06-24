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

package callback

import (
	"context"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// Options holds optional parameters for the Uploader.
type Options struct {
	// httpTimeout specifies the timeout for HTTP requests.
	httpTimeout time.Duration
	// cacheSize specifies the size of the LRU cache for HTTP clients.
	cacheSize int
	// callbackHeaderKey specifies the key using which the callback URL
	// is passed in the headers.
	callbackHeaderKey string
	// callbackURL specifies the URL to which the callback is sent.
	callbackURL string
	// logger is the logger for the publisher.
	logger *zap.SugaredLogger
}

// DefaultOptions returns the default options.
func DefaultOptions(ctx context.Context) *Options {
	return &Options{
		httpTimeout:       10 * time.Second,
		cacheSize:         50,
		logger:            logging.FromContext(ctx),
		callbackHeaderKey: dfv1.KeyMetaCallbackURL,
	}
}

// OptionFunc is a function that applies an option to the Uploader.
type OptionFunc func(*Options)

// WithHTTPTimeout sets the HTTP timeout.
func WithHTTPTimeout(timeout time.Duration) OptionFunc {
	return func(o *Options) {
		o.httpTimeout = timeout
	}
}

// WithLRUCacheSize sets the LRU cache size.
func WithLRUCacheSize(size int) OptionFunc {
	return func(o *Options) {
		o.cacheSize = size
	}
}

// WithCallbackURL sets the callback URL.
func WithCallbackURL(url string) OptionFunc {
	return func(o *Options) {
		o.callbackURL = url
	}
}

// WithLogger sets the logger.
func WithLogger(logger *zap.SugaredLogger) OptionFunc {
	return func(o *Options) {
		o.logger = logger
	}
}
