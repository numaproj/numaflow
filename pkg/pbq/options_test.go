package pbq

import (
	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestOptions(t *testing.T) {
	testOpts := []PBQOption{
		WithReadBatchSize(100),
		WithChannelBufferSize(10),
		WithReadTimeout(2 * time.Second),
		WithPBQStoreOptions(store.WithPbqStoreType(v1alpha1.NoOpType), store.WithStoreSize(1000)),
	}

	queueOption := &options{
		channelBufferSize: 5,
		readTimeout:       1,
		readBatchSize:     5,
		storeOptions:      &store.StoreOptions{},
	}

	for _, opt := range testOpts {
		err := opt(queueOption)
		assert.NoError(t, err)
	}

	assert.Equal(t, int64(100), queueOption.readBatchSize)
	assert.Equal(t, int64(10), queueOption.channelBufferSize)
	assert.Equal(t, 2*time.Second, queueOption.readTimeout)
}
