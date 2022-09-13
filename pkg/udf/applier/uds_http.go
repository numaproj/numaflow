/*
Package udf calls the user defined function. It supports HTTP based invocation and
can be extended to stdin/stdout FIFO etc.
*/

package applier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	funcsdk "github.com/numaproj/numaflow-go/function"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/vmihailenco/msgpack/v5"
)

// UDSHTTPBasedUDF applies user defined function over HTTP (over Unix Domain Socket) client/server where server is the UDF.
type UDSHTTPBasedUDF struct {
	client *http.Client
}

var _ Applier = (*UDSHTTPBasedUDF)(nil)

// options for HTTPBasedUDF
type options struct {
	httpClientTimeout time.Duration
}

// Option to apply different options
type Option interface {
	apply(*options)
}

type httpClientTimeout time.Duration

func (h httpClientTimeout) apply(opts *options) {
	opts.httpClientTimeout = time.Duration(h)
}

// WithHTTPClientTimeout sets timeout for the HTTP Client
func WithHTTPClientTimeout(t time.Duration) Option {
	return httpClientTimeout(t)
}

// NewUDSHTTPBasedUDF returns UDSHTTPBasedUDF.
// Parameter - socketPath, Unix Domain Socket path
func NewUDSHTTPBasedUDF(socketPath string, opts ...Option) *UDSHTTPBasedUDF {
	options := options{
		// default REST standard is 10 seconds
		httpClientTimeout: time.Second * 10,
	}
	for _, o := range opts {
		o.apply(&options)
	}

	var httpClient *http.Client
	// https://www.loginradius.com/blog/async/tune-the-go-http-client-for-high-performance/
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	// all our connects are loop back
	httpTransport.MaxIdleConns = 100
	httpTransport.MaxConnsPerHost = 100
	httpTransport.MaxIdleConnsPerHost = 100
	httpTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return net.Dial("unix", socketPath)
	}
	httpClient = &http.Client{
		Transport: httpTransport,
		Timeout:   options.httpClientTimeout,
	}

	return &UDSHTTPBasedUDF{
		client: httpClient,
	}
}

// Apply applies the user defined function.
func (u *UDSHTTPBasedUDF) Apply(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.Message, error) {
	payload := readMessage.Body.Payload
	offset := readMessage.ReadOffset
	parentPaneInfo := readMessage.PaneInfo

	req, err := http.NewRequestWithContext(ctx, "POST", "http://unix/messages", bytes.NewBuffer(payload))
	// looking at the code, err returned by NewRequestWithContext are InternalErrs which cannot be resolved by retrying.
	// Let's return InternalErr and pause the pipeline.
	if err != nil {
		return nil, ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("http.NewRequestWithContext failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	}
	req.Header.Set(dfv1.UDFApplierMessageKey, string(readMessage.Key))

	// hold results (from response)
	var data []byte
	var contentType dfv1.ContentType

	// retry if we think it is a transient error. maybe we should make this retry more elegant :-)
	i := 0
	retryCount := 3
	var failed = true
retry:
	for i < retryCount && failed {
		i++
		failed = false
		if resp, err := u.client.Do(req); err != nil {
			return nil, ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("client.Do failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		} else {
			data, err = io.ReadAll(resp.Body)
			if err != nil {
				failed = true
				logging.FromContext(ctx).Warnf("io.ReadAll failed (%d/%d), %s", i, retryCount, err)
				goto retry
			}
			_ = resp.Body.Close()
			// TODO: differentiate between 502, 504 from others etc
			if resp.StatusCode >= 300 {
				failed = true
				logging.FromContext(ctx).Warnf("maincar returned statusCode=%d (%d/%d) %s", resp.StatusCode, i, retryCount, data)
				goto retry
			}
			contentType = dfv1.ContentType(resp.Header.Get("Content-Type"))
		}
	}
	// ran out of retry limit
	if failed {
		return nil, ApplyUDFErr{
			UserUDFErr: true,
			Message:    "ran out of retry limit",
			InternalErr: InternalErr{
				Flag:        false,
				MainCarDown: false,
			},
		}
	}
	messages, err := unmarshalMessages(contentType, data)
	if err != nil {
		return nil, err
	}
	writeMessages := []*isb.Message{}
	for i, m := range messages.Items() {
		key := m.Key
		if key == nil {
			key = []byte{}
		}
		writeMessage := &isb.Message{
			Header: isb.Header{
				PaneInfo: parentPaneInfo,
				ID:       fmt.Sprintf("%s-%d", offset.String(), i),
				// TODO: change to use string directly after SDK upgrade
				Key: string(key),
			},
			Body: isb.Body{
				Payload: m.Value,
			},
		}
		writeMessages = append(writeMessages, writeMessage)
	}
	return writeMessages, nil
}

// WaitUntilReady waits till the readyURL is available
func (u *UDSHTTPBasedUDF) WaitUntilReady(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to wait for ready: %w", ctx.Err())
		default:
			if resp, err := u.client.Get("http://unix/ready"); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode < 300 {
					return nil
				}
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func unmarshalMessages(contentType dfv1.ContentType, data []byte) (*funcsdk.Messages, error) {
	messages := &funcsdk.Messages{}
	switch contentType {
	case dfv1.JsonType:
		if err := json.Unmarshal(data, &messages); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data from the response with json, %w", err)
		}
	case dfv1.MsgPackType:
		if err := msgpack.Unmarshal(data, &messages); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data from the response with msgpack, %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported UDF Content-Type %q", string(contentType))
	}
	return messages, nil
}
