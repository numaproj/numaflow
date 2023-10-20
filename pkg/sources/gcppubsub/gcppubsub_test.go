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

package gcppubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/sources/forward/applier"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type myForwardToAllTest struct {
}

func (f myForwardToAllTest) WhereTo(_ []string, _ []string) ([]forward.VertexBuffer, error) {
	return []forward.VertexBuffer{{
		ToVertexName:         "test",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func testVertex(t *testing.T, projectID string, topic string, hostname string, replicaIndex int32) *dfv1.VertexInstance {
	t.Helper()
	v := &dfv1.Vertex{
		Spec: dfv1.VertexSpec{
			AbstractVertex: dfv1.AbstractVertex{
				Name: "test-v",
				Source: &dfv1.Source{
					GCPPubSub: &dfv1.GCPPubSubSource{
						ProjectID: projectID,
						Topic:     topic,
					},
				},
			},
		},
	}
	vi := &dfv1.VertexInstance{
		Vertex:   v,
		Hostname: hostname,
		Replica:  replicaIndex,
	}
	return vi
}

func Test_Single(t *testing.T) {
	// default read timeout is 1 sec, and smaller values seems to be flaky
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	srv := pstest.NewServer()
	defer srv.Close()

	topicID := "test-topic"
	projectID := "test-project"
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	defer conn.Close()
	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConn(conn))
	assert.NoError(t, err)
	defer client.Close()
	ct, err := client.CreateTopic(ctx, topicID)
	assert.NoError(t, err)
	subscription, err := client.CreateSubscription(ctx, "test-subscription", pubsub.SubscriptionConfig{Topic: ct})
	assert.NoError(t, err)

	vi := testVertex(t, projectID, topicID, "test-host", 0)
	dest := simplebuffer.NewInMemoryBuffer("test", 100, 0)
	toBuffers := map[string][]isb.BufferWriter{
		"test": {dest},
	}

	publishWMStores, _ := store.BuildNoOpWatermarkStore()
	fetchWatermark, _ := generic.BuildNoOpWatermarkProgressorsFromBufferMap(map[string][]isb.BufferWriter{})
	toVertexWmStores := map[string]store.WatermarkStore{
		"testVertex": publishWMStores,
	}
	pss, err := New(ctx, cancel, client, subscription, vi, toBuffers, myForwardToAllTest{}, applier.Terminal, fetchWatermark, toVertexWmStores, publishWMStores, wmb.NewIdleManager(len(toBuffers)), WithReadTimeout(1*time.Second))

	assert.NoError(t, err)
	assert.NotNil(t, pss)

	assert.Equal(t, "test-v", pss.GetName())
	defer pss.Close()

	for i := 0; i < 3; i++ {
		result := ct.Publish(ctx, &pubsub.Message{
			Data: []byte(fmt.Sprintf("%d", i)),
		})
		_, err := result.Get(ctx)
		assert.NoError(t, err)
	}

	var msgs []*isb.ReadMessage
	var readMessagesCount int
loop:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for messages")
			return
		default:
			msgs, err = pss.Read(ctx, 5)
			assert.NoError(t, err)
			readMessagesCount += len(msgs)
			if readMessagesCount == 3 {
				break loop
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// go func() {
	// 	err = subscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
	// 		fmt.Println(m.Data)
	// 		m.Ack() // Acknowledge that we've consumed the message.
	// 	})
	// }()
	// assert.NoError(t, err)
}
