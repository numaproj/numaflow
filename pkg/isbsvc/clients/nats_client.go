package clients

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"

	"github.com/nats-io/nats.go"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

// JetStreamClient is used to provide a jetstream client
type JetStreamClient interface {
	Connect(ctx context.Context) (*nats.Conn, error)
}

// inClusterJetStreamClient is used to provide inClusterJetStreamClient credentials
type inClusterJetStreamClient struct {
}

// NewInClusterJetStreamClient is used to provide NewInClusterJetStreamClient
func NewInClusterJetStreamClient() *inClusterJetStreamClient {
	return &inClusterJetStreamClient{}
}

// Connect is used to establish an incluster NATS jetstream connection
func (isc *inClusterJetStreamClient) Connect(ctx context.Context) (*nats.Conn, error) {
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
	opts := []nats.Option{nats.UserInfo(user, password)}
	if sharedutil.LookupEnvStringOr(dfv1.EnvISBSvcJetStreamTLSEnabled, "false") == "true" {
		opts = append(opts, nats.Secure(&tls.Config{
			InsecureSkipVerify: true,
		}))
	}
	return natsJetStreamConnection(ctx, url, opts)
}

// defaultJetStreamClient is used to provide default jetstream client credentials
type defaultJetStreamClient struct {
	url  string
	opts []nats.Option
}

// NewDefaultJetStreamClient is used to provide NewDefaultJetStreamClient
func NewDefaultJetStreamClient(url string, opts ...nats.Option) *defaultJetStreamClient {
	return &defaultJetStreamClient{
		url:  url,
		opts: opts,
	}
}

// Connect is used to establish a default jetstream connection
func (dc *defaultJetStreamClient) Connect(ctx context.Context) (*nats.Conn, error) {
	return natsJetStreamConnection(ctx, dc.url, dc.opts)
}

// natsJetStreamConnection is used to provide a simple NATS JetStream connection using default vars
func natsJetStreamConnection(ctx context.Context, url string, natsOptions []nats.Option) (*nats.Conn, error) {
	log := logging.FromContext(ctx)
	opts := []nats.Option{
		// Retry forever
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Error(err, "Nats connection lost")
			} else {
				log.Info("Nats disconnected")
			}
		}),
		nats.ReconnectHandler(func(nnc *nats.Conn) {
			log.Info("Reconnected to nats server")
		}),
	}

	opts = append(opts, natsOptions...)

	if nc, err := nats.Connect(url, opts...); err != nil {
		return nil, fmt.Errorf("failed to connect to nats url=%s: %w", url, err)
	} else {
		log.Info("Connected to nats server")
		return nc, nil
	}
}
