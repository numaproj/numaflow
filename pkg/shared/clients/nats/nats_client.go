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
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

// Client is a client for NATS server which be shared by multiple connections (reader, writer, kv, buffer management, etc.)
type Client struct {
	sync.Mutex
	nc    *nats.Conn
	jsCtx nats.JetStreamContext
	log   *zap.SugaredLogger
}

// NewNATSClient Create a new NATS client
func NewNATSClient(ctx context.Context, natsOptions ...nats.Option) (*Client, error) {
	log := logging.FromContext(ctx)
	var jsCtx nats.JetStreamContext
	opts := []nats.Option{
		// Enable Nats auto reconnect
		// if max reconnects is set to -1, it will try to reconnect forever
		nats.MaxReconnects(-1),
		// every one second we will try to ping the server, if we don't get a pong back
		// after two attempts, we will consider the connection lost and try to reconnect
		nats.PingInterval(3 * time.Second),
		// error handler for the connection
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			log.Errorw("Nats default: error occurred for subscription", zap.Error(err))
		}),
		// connection closed handler
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Info("Nats default: connection closed")
		}),
		// retry on failed connect should be true, else it wont try to reconnect during initial connect
		nats.RetryOnFailedConnect(true),
		// disconnect handler to log when we lose connection
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Errorw("Nats default: disconnected", zap.Error(err))
		}),
		// reconnect handler to log when we reconnect
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("Nats default: reconnected")
		}),
		// Write (and flush) timeout
		nats.FlusherTimeout(10 * time.Second),

		// If the server doesn't respond to 2 pings we will reconnect
		nats.MaxPingsOutstanding(2),

		// log when the server enters lame duck mode
		nats.LameDuckModeHandler(func(nc *nats.Conn) {
			log.Info("Nats default: entering lame duck mode to avoid reconnect storm")
		}),
	}

	url, existing := os.LookupEnv(dfv1.EnvISBSvcJetStreamURL)
	if !existing {
		return nil, fmt.Errorf("environment variable %q not found", dfv1.EnvISBSvcJetStreamURL)
	}
	user, existing := os.LookupEnv(dfv1.EnvISBSvcJetStreamUser)
	if !existing {
		return nil, fmt.Errorf("environment variable %q not found", dfv1.EnvISBSvcJetStreamUser)
	}
	password, existing := os.LookupEnv(dfv1.EnvISBSvcJetStreamPassword)
	if !existing {
		return nil, fmt.Errorf("environment variable %q not found", dfv1.EnvISBSvcJetStreamPassword)
	}
	// Pass nats options for username password
	opts = append(opts, nats.UserInfo(user, password))
	if sharedutil.LookupEnvStringOr(dfv1.EnvISBSvcJetStreamTLSEnabled, "false") == "true" {
		opts = append(opts, nats.Secure(&tls.Config{
			InsecureSkipVerify: true,
		}))
	}

	opts = append(opts, natsOptions...)
	if nc, err := nats.Connect(url, opts...); err != nil {
		return nil, fmt.Errorf("failed to connect to nats url=%s: %w", url, err)
	} else {
		jsCtx, err = nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to create to nats jetstream context: %w", err)
		}
		return &Client{nc: nc, jsCtx: jsCtx, log: logging.FromContext(ctx)}, nil
	}
}

// Subscribe returns a subscription for the given subject and stream
func (c *Client) Subscribe(subject string, stream string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	var (
		err       error
		jsContext nats.JetStreamContext
	)

	jsContext, err = c.nc.JetStream()
	if err != nil {
		return nil, err
	}
	// we use pull subscribe.
	return jsContext.PullSubscribe(subject, stream, opts...)
}

// BindKVStore lookup and bind to an existing KeyValue store and return the KeyValue interface
func (c *Client) BindKVStore(kvName string) (nats.KeyValue, error) {
	var (
		err       error
		jsContext nats.JetStreamContext
	)

	jsContext, err = c.nc.JetStream()
	if err != nil {
		return nil, err
	}

	return jsContext.KeyValue(kvName)
}

// PendingForStream returns the number of pending messages for the given consumer and stream
func (c *Client) PendingForStream(consumer string, stream string) (int64, error) {
	var (
		err   error
		cInfo *nats.ConsumerInfo
	)
	// We only need lock for this function, because we are using a common js context
	c.Lock()
	defer c.Unlock()

	cInfo, err = c.jsCtx.ConsumerInfo(consumer, stream)
	if err != nil {
		return dfv1.PendingNotAvailable, fmt.Errorf("failed to get consumer info, %w", err)
	}
	return int64(cInfo.NumPending) + int64(cInfo.NumAckPending), nil
}

// JetStreamContext returns a new JetStreamContext
func (c *Client) JetStreamContext(opts ...nats.JSOpt) (nats.JetStreamContext, error) {
	return c.nc.JetStream(opts...)
}

// Close closes the NATS client
func (c *Client) Close() {
	c.nc.Close()
}

// NewTestClient creates a new NATS client for testing
// only use this for testing
func NewTestClient(t *testing.T, url string) *Client {
	nc, err := nats.Connect(url)
	if err != nil {
		panic(err)
	}
	return &Client{nc: nc}
}

// JetStreamClient is used to get a testing JetStream client instance
func NewTestClientWithServer(t *testing.T, s *server.Server) *Client {
	return NewTestClient(t, s.ClientURL())
}
