package nats

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func TestNewNATSClient(t *testing.T) {
	// Setting up environment variables for the test
	os.Setenv(dfv1.EnvISBSvcJetStreamURL, "nats://localhost:4222")
	os.Setenv(dfv1.EnvISBSvcJetStreamUser, "user")
	os.Setenv(dfv1.EnvISBSvcJetStreamPassword, "password")
	defer os.Clearenv()

	log := zap.NewNop().Sugar()

	ctx := logging.WithLogger(context.Background(), log)

	client, err := NewNATSClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	// Cleanup
	client.Close()
}

func TestNewNATSClient_Failure(t *testing.T) {
	// Simulating environment variable absence
	os.Clearenv()

	log := zap.NewNop().Sugar()
	ctx := logging.WithLogger(context.Background(), log)

	client, err := NewNATSClient(ctx)
	assert.Error(t, err)
	assert.Nil(t, client)
}
