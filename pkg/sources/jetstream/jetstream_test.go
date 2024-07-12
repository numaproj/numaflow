package jetstream

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
)

func TestJetstreamRead(t *testing.T) {
	ctx := context.Background()
	s := test.RunJetStreamServer(t)
	defer test.ShutdownJetStreamServer(t, s)

	// write some messages to the stream
	conn, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	defer conn.Close()

	js, err := jetstream.New(conn)
	assert.NoError(t, err)

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "test-stream",
		Subjects: []string{"test-stream"},
	})
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := js.Publish(ctx, "test-stream", []byte("test-message"))
		assert.NoError(t, err)
	}

	vertexInstance := &dfv1.VertexInstance{
		Vertex: &dfv1.Vertex{
			Spec: dfv1.VertexSpec{
				PipelineName: "testPipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "testVertex",
					Source: &dfv1.Source{
						JetStream: &dfv1.JetStreamSource{
							Stream: "test-stream",
							URL:    s.ClientURL(),
						},
					},
				},
			},
		},
	}

	jsSource, err := New(ctx, vertexInstance)
	assert.NoError(t, err)
	defer jsSource.Close()

	// read messages
	messages, err := jsSource.Read(ctx, 10)
	assert.NoError(t, err)

	assert.Len(t, messages, 10)
}

func TestJetstreamSourceServingEnabled(t *testing.T) {
	ctx := context.Background()
	s := test.RunJetStreamServer(t)
	defer test.ShutdownJetStreamServer(t, s)

	// write some messages to the stream
	conn, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	defer conn.Close()

	js, err := jetstream.New(conn)
	assert.NoError(t, err)

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "test-stream",
		Subjects: []string{"test-stream"},
	})
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := js.Publish(ctx, "test-stream", []byte("test-message"))
		assert.NoError(t, err)
	}

	t.Setenv(dfv1.EnvISBSvcJetStreamURL, s.ClientURL())
	t.Setenv(dfv1.EnvServingJetstreamStream, "test-stream")
	t.Setenv(dfv1.EnvISBSvcJetStreamUser, "")
	t.Setenv(dfv1.EnvISBSvcJetStreamPassword, "")

	vertexInstance := &dfv1.VertexInstance{
		Vertex: &dfv1.Vertex{
			Spec: dfv1.VertexSpec{
				PipelineName: "testPipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "testVertex",
					Source: &dfv1.Source{
						Serving: &dfv1.Serving{},
					},
				},
			},
		},
	}

	jsSource, err := New(ctx, vertexInstance, WithServingEnabled())
	assert.NoError(t, err)

	// read messages
	messages, err := jsSource.Read(ctx, 10)
	assert.NoError(t, err)

	assert.Len(t, messages, 10)
}
