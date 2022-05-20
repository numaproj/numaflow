// Package golang provides an interface to write UDF in golang which will be exposed over HTTP. It accepts a handler of the following definition
//  func(ctx context.Context, key, msg []byte) (messages Messages, err error)
// which will be invoked for message. If error is returned, the HTTP StatusCode will be set to 500.
package function

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	contentTypeJson    = "application/json"
	contentTypeMsgPack = "application/msgpack"

	envUDFContentType = "NUMAFLOW_UDF_CONTENT_TYPE"

	messagekey = "x-numa-message-key"
)

type Handle func(ctx context.Context, key, msg []byte) (Messages, error)

// options for starting the http udf server
type options struct {
	drainTimeout time.Duration
}

// Option to apply different options
type Option interface {
	apply(*options)
}

type drainTimeout time.Duration

func (f drainTimeout) apply(opts *options) {
	opts.drainTimeout = time.Duration(f)
}

// WithDrainTimeout sets a max drain timeout time. It is the maximum time we will wait for the connection to drain out once we have
// initiated the shutdown sequence. Default is 1 minute.
func WithDrainTimeout(f time.Duration) Option {
	return drainTimeout(f)
}

// Start starts the HTTP Server after registering the handler at `/messages` endpoint.
func Start(ctx context.Context, handler Handle, opts ...Option) {
	options := options{
		drainTimeout: time.Minute,
	}

	for _, o := range opts {
		o.apply(&options)
	}

	ctxWithSignal, stop := signal.NotifyContext(ctx, syscall.SIGTERM)
	defer stop()
	if err := startWithContext(ctxWithSignal, handler, options); err != nil {
		panic(err)
	}
}

func udf(ctx context.Context, w http.ResponseWriter, r *http.Request, handler func(ctx context.Context, key, msg []byte) (Messages, error), contentType string) {
	messages, err := func() ([]Message, error) {
		k := r.Header.Get(messagekey)
		in, err := ioutil.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			return nil, err
		} else {
			return handler(ctx, []byte(k), in)
		}
	}()
	if err != nil {
		log.Printf("Failed to read and process input message, %s", err)
		w.WriteHeader(500)
		_, _ = w.Write([]byte(err.Error()))
	} else {
		if len(messages) == 0 { // Return a DROP message
			messages = append(messages, MessageToDrop())
		}
		b, err := marshalMessages(messages, contentType)
		if err != nil {
			log.Printf("Marshal message failed, %s", err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
		} else {
			w.Header().Add("Content-Type", contentType)
			w.WriteHeader(200)
			n, err := w.Write(b)
			if err != nil {
				log.Printf("Write failed (wrote: %d bytes), %s", n, err)
			}
		}
	}
}

func marshalMessages(messages Messages, contentType string) ([]byte, error) {
	switch contentType {
	case contentTypeJson:
		b, err := json.Marshal(&messages)
		if err != nil {
			return nil, fmt.Errorf("marshal messages with json failed, %w", err)
		}
		return b, nil
	case contentTypeMsgPack:
		b, err := msgpack.Marshal(&messages)
		if err != nil {
			return nil, fmt.Errorf("marshal messages with msgpack failed, %w", err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported Content-Type %q", contentType)
	}
}

func startWithContext(ctx context.Context, handler func(ctx context.Context, key, msg []byte) (Messages, error), opts options) error {
	contentType := os.Getenv(envUDFContentType)
	if contentType == "" { // defaults to application/msgpack
		contentType = contentTypeMsgPack
	}
	if contentType != contentTypeJson && contentType != contentTypeMsgPack {
		return fmt.Errorf("unsupported Content-Type %q", contentType)
	}
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})
	http.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		udf(ctx, w, r, handler, contentType)
	})

	path := "/var/run/numaflow/udf.sock"
	if err := os.Remove(path); !os.IsNotExist(err) && err != nil {
		return err
	}
	udsServer := &http.Server{}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	go func() {
		if err := udsServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	log.Printf("udf server is ready")

	// wait for signal
	<-ctx.Done()
	log.Println("udf server is now shutting down")
	defer log.Println("udf server has exited")

	// let's not wait indefinitely
	stopCtx, cancel := context.WithTimeout(context.Background(), opts.drainTimeout)
	defer cancel()
	if err := udsServer.Shutdown(stopCtx); err != nil {
		return err
	}

	return nil
}
