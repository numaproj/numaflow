package sink

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
)

type Handle func(context.Context, []Message) (Responses, error)

// options for starting the http udf server
type options struct {
	drainTimeout time.Duration
}

// Option to apply different options
type Option func(*options)

// WithDrainTimeout sets a max drain timeout time. It is the maximum time we will wait for the connection to drain out once we have
// initiated the shutdown sequence. Default is 1 minute.
func WithDrainTimeout(t time.Duration) Option {
	return func(o *options) {
		o.drainTimeout = t
	}
}

// Start starts the HTTP Server after registering the handler at `/messages` endpoint.
func Start(ctx context.Context, handler Handle, opts ...Option) {
	options := &options{
		drainTimeout: time.Minute,
	}
	for _, o := range opts {
		o(options)
	}

	ctxWithSignal, stop := signal.NotifyContext(ctx, syscall.SIGTERM)
	defer stop()
	if err := startWithContext(ctxWithSignal, handler, options); err != nil {
		panic(err)
	}
}

func udsink(ctx context.Context, w http.ResponseWriter, r *http.Request, handler func(ctx context.Context, msgs []Message) (Responses, error)) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" { // defaults to application/msgpack
		contentType = contentTypeMsgPack
	}
	if contentType != contentTypeJson && contentType != contentTypeMsgPack {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("unsupported Content-Type " + contentType))
		return
	}

	msgResponses, err := func() (Responses, error) {
		in, err := ioutil.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			return nil, err
		} else {
			msgs, err := unmarshalMessages(in, contentType)
			if err != nil {
				return nil, err
			}
			return handler(ctx, msgs)
		}
	}()
	if err != nil {
		log.Printf("Failed to read or process the messages, %s", err)
		w.WriteHeader(500)
		_, _ = w.Write([]byte(err.Error()))
	} else if len(msgResponses) > 0 {
		b, err := marshalMessageResponses(msgResponses, contentType)
		if err != nil {
			log.Printf("Marshal message responses failed, %s", err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		w.Header().Add("Content-Type", contentType)
		w.WriteHeader(200)
		n, err := w.Write(b)
		if err != nil {
			log.Printf("Write failed (wrote: %d bytes), %s", n, err)
		}
	} else {
		w.WriteHeader(204)
	}
}

func unmarshalMessages(data []byte, contentType string) ([]Message, error) {
	msgs := []Message{}
	switch contentType {
	case contentTypeJson:
		if err := json.Unmarshal(data, &msgs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data from the request with json, %w", err)
		}
	case contentTypeMsgPack:
		if err := msgpack.Unmarshal(data, &msgs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data from the request with msgpack, %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported UDSink Content-Type %q", contentType)
	}
	return msgs, nil
}

func marshalMessageResponses(respones []Response, contentType string) ([]byte, error) {
	switch contentType {
	case contentTypeJson:
		b, err := json.Marshal(&respones)
		if err != nil {
			return nil, fmt.Errorf("marshal message responses with json failed, %w", err)
		}
		return b, nil
	case contentTypeMsgPack:
		b, err := msgpack.Marshal(&respones)
		if err != nil {
			return nil, fmt.Errorf("marshal message responses with msgpack failed, %w", err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported Content-Type %q", contentType)
	}
}

func startWithContext(ctx context.Context, handler func(ctx context.Context, msgs []Message) (Responses, error), opts *options) error {
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})
	http.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		udsink(ctx, w, r, handler)
	})

	path := "/var/run/numaflow/udsink.sock"
	if err := os.Remove(path); !os.IsNotExist(err) && err != nil {
		return err
	}
	udsinkServer := &http.Server{}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	go func() {
		if err := udsinkServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	log.Printf("udsink server is ready")

	// wait for signal
	<-ctx.Done()
	log.Println("udsink server is now shutting down")
	defer log.Println("udsink server has exited")

	// let's not wait indefinitely
	stopCtx, cancel := context.WithTimeout(context.Background(), opts.drainTimeout)
	defer cancel()
	if err := udsinkServer.Shutdown(stopCtx); err != nil {
		return err
	}

	return nil
}
