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

package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// defaultJetStreamClient is used to provide default jetstream client
type defaultJetStreamClient struct {
	url  string
	opts []nats.Option
}

// NewDefaultJetStreamClient is used to get a default JetStream client instance
func NewDefaultJetStreamClient(url string, opts ...nats.Option) *defaultJetStreamClient {
	return &defaultJetStreamClient{
		url:  url,
		opts: opts,
	}
}

// Connect is used to establish a default jetstream connection
func (dc *defaultJetStreamClient) Connect(ctx context.Context, opts ...JetStreamClientOption) (*NatsConn, error) {
	nc, err := natsJetStreamConnection(ctx, dc.url, dc.opts)
	if err != nil {
		return nil, err
	}
	return NewNatsConn(nc), nil
}

// natsJetStreamConnection is used to provide a simple NATS JetStream connection using default vars
func natsJetStreamConnection(ctx context.Context, url string, natsOptions []nats.Option) (*nats.Conn, error) {
	log := logging.FromContext(ctx)
	opts := []nats.Option{
		// Enable Nats auto reconnect
		// Retry forever
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Error(err, "Nats: connection lost")
			} else {
				log.Info("Nats: disconnected")
			}
		}),
		nats.ReconnectHandler(func(nnc *nats.Conn) {
			log.Info("Nats: reconnected to nats server")
		}),
		// Write (and flush) timeout
		nats.FlusherTimeout(10 * time.Second),
	}

	opts = append(opts, natsOptions...)
	if nc, err := nats.Connect(url, opts...); err != nil {
		return nil, fmt.Errorf("failed to connect to nats url=%s: %w", url, err)
	} else {
		log.Info("Nats: connected to nats server")
		return nc, nil
	}
}
