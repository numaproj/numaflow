package jetstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

// inClusterJetStreamClient is used to provide inClusterJetStreamClient credentials
type inClusterJetStreamClient struct {
}

// NewInClusterJetStreamClient is used to provide NewInClusterJetStreamClient
func NewInClusterJetStreamClient() *inClusterJetStreamClient {
	return &inClusterJetStreamClient{}
}

// Function to get a nats connection
func (isc *inClusterJetStreamClient) connect(ctx context.Context, opts ...JetStreamClientOption) (*nats.Conn, error) {
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
	// pass nats options for username password
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
	nc, err := isc.connect(ctx, opts...)
	if err != nil {
		return nil, err
	}
	result := NewNatsConn(nc)
	options := defaultJetStreamClientOptions()
	for _, o := range opts {
		if o != nil {
			o(options)
		}
	}
	log := logging.FromContext(ctx)
	if options.reconnect { // Start auto reconnection daemon
		go func() {
			log.Info("Starting Nats JetStream auto reconnection daemon...")
			ticker := time.NewTicker(options.connectionCheckInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if !result.IsConnected() {
						log.Info("88888888888")
						if options.disconnectHandler != nil {
							options.disconnectHandler(result, fmt.Errorf("connection lost"))
						}
						conn, err := isc.connect(ctx, opts...)
						if err != nil {
							log.Errorw("Failed to reconnect", zap.Error(err))
							continue
						}
						result.Conn = conn
						result.reloadContexts()
						log.Info("Succeeded to reconnect to Nat JetStream server")
						if options.reconnectHandler != nil {
							options.reconnectHandler(result)
						}
					} else {
						log.Info("9999999999")
					}
				case <-ctx.Done():
					log.Info("Exiting Nats JetStream auto reconnection daemon...")
					return
				}
			}
		}()
	} else {
		log.Info("Nats JetStream auto reconnection is not enabled.")
	}
	return result, nil
}
