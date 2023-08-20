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

// watermarkStoreWatcher defines a pair of heartbeatStoreWatcher and offsetTimelineStoreWatcher,
// it implements interface WatermarkStoreWatcher
type watermarkStoreWatcher struct {
	heartbeatStoreWatcher      kvs.KVWatcher
	offsetTimelineStoreWatcher kvs.KVWatcher
}

var _ WatermarkStoreWatcher = (*watermarkStoreWatcher)(nil)

func (w *watermarkStoreWatcher) HeartbeatWatcher() kvs.KVWatcher {
	return w.heartbeatStoreWatcher
}
func (w *watermarkStoreWatcher) OffsetTimelineWatcher() kvs.KVWatcher {
	return w.offsetTimelineStoreWatcher
}

// BuildNoOpWatermarkStoreWatcher returns a NoOp WatermarkStoreWatcher instance
func BuildNoOpWatermarkStoreWatcher() (WatermarkStoreWatcher, error) {
	return &watermarkStoreWatcher{
		heartbeatStoreWatcher:      noopkv.NewKVOpWatch(),
		offsetTimelineStoreWatcher: noopkv.NewKVOpWatch(),
	}, nil
}

// BuildInmemWatermarkStoreWatcher returns an in-mem WatermarkStoreWatcher instance
func BuildInmemWatermarkStoreWatcher(ctx context.Context, bucket string, hbKVEntryCh, otKVEntryCh <-chan kvs.KVEntry) (WatermarkStoreWatcher, error) {
	hbWatcher, _ := inmem.NewInMemWatch(ctx, bucket+"_PROCESSORS", hbKVEntryCh)
	otWatcher, _ := inmem.NewInMemWatch(ctx, bucket+"_OT", otKVEntryCh)
	return &watermarkStoreWatcher{
		heartbeatStoreWatcher:      hbWatcher,
		offsetTimelineStoreWatcher: otWatcher,
	}, nil
}

// BuildJetStreamWatermarkStoreWatcher returns a JetStream WatermarkStoreWatcher instance
func BuildJetStreamWatermarkStoreWatcher(ctx context.Context, bucket string, client *jsclient.NATSClient) (WatermarkStoreWatcher, error) {
	hbKVName := JetStreamProcessorKVName(bucket)
	hbWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, hbKVName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new JetStream HB KV watch for %q, %w", hbKVName, err)
	}

	otKVName := JetStreamOTKVName(bucket)
	otWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, otKVName, client)
	if err != nil {
		return nil, fmt.Errorf("failed at new JetStream OT KV watch for %q, %w", otKVName, err)
	}
	return &watermarkStoreWatcher{
		heartbeatStoreWatcher:      hbWatch,
		offsetTimelineStoreWatcher: otWatch,
	}, nil
}
