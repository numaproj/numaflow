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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestOptions(t *testing.T) {
	ctx := context.Background()

	// Create default options
	opts := DefaultOptions(ctx)

	// Check default values
	assert.Equal(t, 10*time.Second, opts.httpTimeout)
	assert.Equal(t, 50, opts.cacheSize)
	assert.NotNil(t, opts.logger)

	// Modify options
	WithHTTPTimeout(20 * time.Second)(opts)
	WithLRUCacheSize(100)(opts)
	WithCallbackURL("http://example.com")(opts)
	WithLogger(zap.NewNop().Sugar())(opts)

	// Check modified values
	assert.Equal(t, 20*time.Second, opts.httpTimeout)
	assert.Equal(t, 100, opts.cacheSize)
	assert.Equal(t, "http://example.com", opts.callbackURL)
	assert.IsType(t, &zap.SugaredLogger{}, opts.logger)
}
