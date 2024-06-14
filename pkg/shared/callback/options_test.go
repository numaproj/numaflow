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
