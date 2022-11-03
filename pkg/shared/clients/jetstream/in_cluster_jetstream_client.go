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

package jetstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

// inClusterJetStreamClient is a client which is expected to be only used in a K8s cluster,
// where some environment variables for connection are available.
type inClusterJetStreamClient struct {
}

// NewInClusterJetStreamClient return an instance of inClusterJetStreamClient
func NewInClusterJetStreamClient() *inClusterJetStreamClient {
	return &inClusterJetStreamClient{}
}

// Function to get a nats connection
func (isc *inClusterJetStreamClient) connect(ctx context.Context) (*nats.Conn, error) {
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
	natsOpts := []nats.Option{nats.UserInfo(user, password)}
	if sharedutil.LookupEnvStringOr(dfv1.EnvISBSvcJetStreamTLSEnabled, "false") == "true" {
		natsOpts = append(natsOpts, nats.Secure(&tls.Config{
			InsecureSkipVerify: true,
		}))
	}
	return natsJetStreamConnection(ctx, url, natsOpts)
}

// Connect is used to establish an incluster NATS jetstream connection
func (isc *inClusterJetStreamClient) Connect(ctx context.Context, opts ...JetStreamClientOption) (*NatsConn, error) {
	options := defaultJetStreamClientOptions()
	for _, o := range opts {
		if o != nil {
			o(options)
		}
	}
	nc, err := isc.connect(ctx)
	if err != nil {
		return nil, err
	}
	natsConn := NewNatsConn(nc)
	log := logging.FromContext(ctx)
	if options.reconnect {
		// Start auto reconnection daemon.
		// Raw Nats auto reconnection is not always working
		go func() {
			log.Info("Starting Nats JetStream auto reconnection daemon...")
			defer log.Info("Exited Nats JetStream auto reconnection daemon...")
			wait.JitterUntilWithContext(ctx, func(ctx context.Context) {
				if !natsConn.IsConnected() {
					log.Info("Nats JetStream connection lost")
					if options.disconnectHandler != nil {
						options.disconnectHandler(natsConn, fmt.Errorf("connection lost"))
					}
					conn, err := isc.connect(ctx)
					if err != nil {
						log.Errorw("Failed to reconnect", zap.Error(err))
						return
					}
					natsConn.Conn = conn
					natsConn.reloadContexts()
					log.Info("Succeeded to reconnect to Nat JetStream server")
					if options.reconnectHandler != nil {
						options.reconnectHandler(natsConn)
					}
				} else {
					log.Debug("Nats JetStream connection is good")
				}
			}, options.connectionCheckInterval, 1.1, true)
		}()
	} else {
		log.Info("Nats JetStream auto reconnection is not enabled.")
	}
	return natsConn, nil
}
