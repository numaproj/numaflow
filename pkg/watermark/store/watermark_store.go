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

package store

import (
	"context"
	"fmt"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/kvs/inmem"
	"github.com/numaproj/numaflow/pkg/shared/kvs/jetstream"
	noopkv "github.com/numaproj/numaflow/pkg/shared/kvs/noop"
)

// watermarkStore wraps a pair of heartbeatStore and offsetTimelineStore,
// it implements interface WatermarkStore.
type watermarkStore struct {
	heartbeatStore      kvs.KVStorer
	offsetTimelineStore kvs.KVStorer
}

var _ WatermarkStore = (*watermarkStore)(nil)

func (ws *watermarkStore) HeartbeatStore() kvs.KVStorer {
	return ws.heartbeatStore
}

func (ws *watermarkStore) OffsetTimelineStore() kvs.KVStorer {
	return ws.offsetTimelineStore
}

func (ws *watermarkStore) Close() error {
	ws.heartbeatStore.Close()
	ws.offsetTimelineStore.Close()
	return nil
}

// BuildNoOpWatermarkStore returns a NoOp WatermarkStore instance
func BuildNoOpWatermarkStore() (WatermarkStore, error) {
	return &watermarkStore{
		heartbeatStore:      noopkv.NewKVNoOpStore(),
		offsetTimelineStore: noopkv.NewKVNoOpStore(),
	}, nil
}

// BuildInmemWatermarkStore returns an in-mem WatermarkStore instance, and the HB, OT entry channels
func BuildInmemWatermarkStore(ctx context.Context, bucket string) (WatermarkStore, error) {
	hbKV, _ := inmem.NewKVInMemKVStore(ctx, bucket+"_PROCESSORS")
	otKV, _ := inmem.NewKVInMemKVStore(ctx, bucket+"_OT")
	return &watermarkStore{
		heartbeatStore:      hbKV,
		offsetTimelineStore: otKV,
	}, nil
}

// BuildJetStreamWatermarkStore returns a JetStream WatermarkStore instance
func BuildJetStreamWatermarkStore(ctx context.Context, bucket string, client *jsclient.NATSClient) (WatermarkStore, error) {
	// build heartBeat store
	hbKVName := JetStreamProcessorKVName(bucket)
	hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, hbKVName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new JetStream HB KV store %q, %w", hbKVName, err)
	}

	// build offsetTimeline store
	otStoreKVName := JetStreamOTKVName(bucket)
	otStore, err := jetstream.NewKVJetStreamKVStore(ctx, otStoreKVName, client)
	if err != nil {
		hbStore.Close()
		return nil, fmt.Errorf("failed at new JetStream OT KV store %q, %w", otStoreKVName, err)
	}
	return &watermarkStore{
		heartbeatStore:      hbStore,
		offsetTimelineStore: otStore,
	}, nil
}

func JetStreamProcessorKVName(bucketName string) string {
	return fmt.Sprintf("%s_PROCESSORS", bucketName)
}

func JetStreamOTKVName(bucketName string) string {
	return fmt.Sprintf("%s_OT", bucketName)
}
