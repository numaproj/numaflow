package udsink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	sinksdk "github.com/numaproj/numaflow-go/sink"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/vmihailenco/msgpack/v5"
)

type udsHTTPBasedUDSink struct {
	client      *http.Client
	contentType dfv1.ContentType
}

// options for HTTPBasedUDF
type options struct {
	httpClientTimeout time.Duration
	contentType       dfv1.ContentType
}

type udsSinkOption func(o *options)

func withTimeout(t time.Duration) udsSinkOption {
	return func(o *options) {
		o.httpClientTimeout = t
	}
}

func withContentType(t dfv1.ContentType) udsSinkOption {
	return func(o *options) {
		o.contentType = t
	}
}

func NewUDSHTTPBasedUDSink(socketPath string, opts ...udsSinkOption) *udsHTTPBasedUDSink {
	options := &options{
		httpClientTimeout: time.Second * 10,
		contentType:       dfv1.MsgPackType,
	}
	for _, o := range opts {
		o(options)
	}
	var httpClient *http.Client
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
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
	return &udsHTTPBasedUDSink{
		client:      httpClient,
		contentType: options.contentType,
	}
}

func (u *udsHTTPBasedUDSink) Apply(ctx context.Context, msgs []sinksdk.Message) []error {
	errs := make([]error, len(msgs))
	payload, err := marshalMessages(u.contentType, msgs)
	if err != nil {
		for i := range msgs {
			errs[i] = ApplyUDSinkErr{
				UserUDSinkErr: false,
				Message:       fmt.Sprintf("marshal messages failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
		return errs
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "http://unix/messages", bytes.NewBuffer(payload))
	if err != nil {
		for i := range msgs {
			errs[i] = ApplyUDSinkErr{
				UserUDSinkErr: false,
				Message:       fmt.Sprintf("http.NewRequestWithContext failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
		return errs
	}
	req.Header.Set("Content-Type", string(u.contentType))

	// hold results (from response)
	var data []byte
	var responseContentType dfv1.ContentType

	i := 0
	retryCount := 3
	var failed = true
retry:
	for i < retryCount && failed {
		i++
		failed = false
		if resp, err := u.client.Do(req); err != nil {
			for i := range msgs {
				errs[i] = ApplyUDSinkErr{
					UserUDSinkErr: false,
					Message:       fmt.Sprintf("client.Do failed, %s", err),
					InternalErr: InternalErr{
						Flag:        true,
						MainCarDown: false,
					},
				}
			}
			return errs
		} else {
			data, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				failed = true
				logging.FromContext(ctx).Warnf("ioutil.ReadAll failed (%d/%d), %w", i, retryCount, err)
				goto retry
			}
			_ = resp.Body.Close()
			if resp.StatusCode >= 300 {
				failed = true
				logging.FromContext(ctx).Warnf("maincar returned statusCode=%d (%d/%d) %s", resp.StatusCode, i, retryCount, data)
				goto retry
			}
			responseContentType = dfv1.ContentType(resp.Header.Get("Content-Type"))
		}
	}
	// ran out of retry limit
	if failed {
		for i := range msgs {
			errs[i] = ApplyUDSinkErr{
				UserUDSinkErr: true,
				Message:       "ran out of retry limit",
				InternalErr: InternalErr{
					Flag:        false,
					MainCarDown: false,
				},
			}
		}
		return errs
	}

	responses, err := unmarshalMessageResponses(responseContentType, data)
	if err != nil {
		for i := range msgs {
			errs[i] = ApplyUDSinkErr{
				UserUDSinkErr: false,
				Message:       fmt.Sprintf("failed to unmarshal message responses, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
		return errs
	}
	// Use ID to map the response messages, so that there's no strict requirement for the user defined sink to return the responses in order.
	resMap := make(map[string]sinksdk.Response)
	for _, res := range responses.Items() {
		resMap[res.ID] = res
	}
	for i, m := range msgs {
		if r, existing := resMap[m.ID]; !existing {
			errs[i] = fmt.Errorf("not found in responses")
		} else {
			if !r.Success {
				if r.Err != "" {
					errs[i] = fmt.Errorf(r.Err)
				} else {
					errs[i] = fmt.Errorf("unsuccessful due to unknown reason")
				}
			}
		}
	}
	return errs
}

// WaitUntilReady waits till the readyURL is available
func (u *udsHTTPBasedUDSink) WaitUntilReady(ctx context.Context) error {
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

func marshalMessages(contentType dfv1.ContentType, msgs []sinksdk.Message) ([]byte, error) {
	switch contentType {
	case dfv1.JsonType:
		return json.Marshal(msgs)
	case dfv1.MsgPackType:
		return msgpack.Marshal(msgs)
	default:
		return nil, fmt.Errorf("unsupported UDSink Content-Type %q", string(contentType))
	}
}

func unmarshalMessageResponses(contentType dfv1.ContentType, data []byte) (sinksdk.Responses, error) {
	responses := sinksdk.Responses{}
	switch contentType {
	case dfv1.JsonType:
		if err := json.Unmarshal(data, &responses); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data from the response with json, %w", err)
		}
	case dfv1.MsgPackType:
		if err := msgpack.Unmarshal(data, &responses); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data from the response with msgpack, %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported UDSink Content-Type %q", string(contentType))
	}
	return responses, nil
}
