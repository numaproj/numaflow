package progress

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
)

func GetJetStreamConnection(ctx context.Context) (nats.JetStreamContext, error) {
	inClusterClient := clients.NewInClusterJetStreamClient()
	var err error
	conn, err := inClusterClient.Connect(ctx)
	if err != nil {
		return nil, err
	}
	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}

	return js, nil
}

// GetHeartbeatBucket returns the heartbeat bucket.
func GetHeartbeatBucket(js nats.JetStreamContext, publishKeyspace string) (nats.KeyValue, error) {
	return js.KeyValue(publishKeyspace + "_PROCESSORS")
}

// GetFetchKeyspace gets the fetch keyspace name from the vertex.
func GetFetchKeyspace(v *dfv1.Vertex) string {
	// from vertices is 0 because we do not support diamond DAG
	return fmt.Sprintf("%s-%s-%s", v.Namespace, v.Spec.PipelineName, v.Spec.FromVertices[0])
}

// GetPublishKeySpace gets the publish keyspace name from the vertex
func GetPublishKeySpace(v *dfv1.Vertex) string {
	return fmt.Sprintf("%s-%s-%s", v.Namespace, v.Spec.PipelineName, v.Spec.Name)
}

// CreateProcessorBucketIfMissing creates the KV bucket if missing
// TODO: this should be moved to controller
func CreateProcessorBucketIfMissing(bucketName string, js nats.JetStreamContext) (err error) {
	_, err = js.KeyValue(bucketName)
	if err != nil {
		if errors.Is(err, nats.ErrBucketNotFound) {
			_, err = js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:       bucketName,
				Description:  bucketName,
				MaxValueSize: 0,
				History:      0,
				TTL:          0,
				MaxBytes:     0,
				Storage:      0,
				Replicas:     0,
				Placement:    nil,
			})
		}
	}

	return err
}
