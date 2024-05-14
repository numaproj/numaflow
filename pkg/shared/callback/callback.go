package callback

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
)

// Publisher is the callback publisher which sends POST requests to callback URLs.
type Publisher struct {
	vertexName   string
	pipelineName string
	clientsCache *lru.Cache[string, *http.Client]
	opts         *Options
}

// NewPublisher creates a new callback publisher
func NewPublisher(ctx context.Context, vertexName string, pipelineName string, opts ...OptionFunc) *Publisher {
	dOpts := DefaultOptions(ctx)
	for _, opt := range opts {
		opt(dOpts)
	}

	clientCache, _ := lru.NewWithEvict[string, *http.Client](dOpts.cacheSize, func(key string, value *http.Client) {
		// Close the client when it's removed from the cache
		value.CloseIdleConnections()
	})

	if dOpts.callbackURL != "" {
		client := &http.Client{
			Timeout: dOpts.httpTimeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // Set InsecureSkipVerify to true
				},
			},
		}
		clientCache.Add(dOpts.callbackURL, client)
	}

	return &Publisher{
		vertexName:   vertexName,
		pipelineName: pipelineName,
		clientsCache: clientCache,
		opts:         dOpts,
	}
}

// Request is the struct that holds the data to be sent in the POST request
type Request struct {
	// Vertex is the name of the vertex
	Vertex string `json:"vertex"`
	// Pipeline is the name of the pipeline
	Pipeline string `json:"pipeline"`
	// UUID is the unique identifier of the message
	ID string `json:"id"`
	// EventTime is the time when the message was created
	EventTime int64 `json:"event_time"`
	// CallbackTime is the time when the callback was made
	CallbackTime int64 `json:"callback_time"`
	// Tags is the list of tags associated with the message
	Tags []string `json:"tags,omitempty"` // Add 'omitempty' here
	// FromVertex is the name of the vertex from which the message was sent
	FromVertex string `json:"from_vertex"`
}

// NonSinkVertexCallback groups messages based on their callback URL and sends a POST request for each group
// for non-sink vertices. If the callbackHeaderKey is set, it writes all the callback requests to the callbackURL.
// In case of failure while writing the url from the headers, it writes all the callback requests to the callbackURL.
func (cp *Publisher) NonSinkVertexCallback(ctx context.Context, messagePairs []isb.ReadWriteMessagePair) error {
	// Create a map to hold groups of messagePairs for each callback URL
	callbackUrlMap := make(map[string][]Request)

	// Iterate through each pair
	for _, pair := range messagePairs {

		for _, msg := range pair.WriteMessages {
			var callbackURL string

			if cp.opts.callbackHeaderKey == "" {
				callbackURL = cp.opts.callbackURL
			} else {
				// Extract Callback URL from read message headers
				var ok bool
				callbackURL, ok = pair.ReadMessage.Headers[cp.opts.callbackHeaderKey]
				if !ok {
					// Return an error if Callback URL is not found in pair headers
					return errors.New(fmt.Sprintf("Callback URL not found in message headers for key: %s", cp.opts.callbackHeaderKey))
				}
			}

			// Extract UUID from pair headers
			uuid, ok := pair.ReadMessage.Headers["uuid"]
			if !ok {
				// Return an error if UUID is not found in pair headers
				return errors.New(fmt.Sprintf("UUID not found in message headers"))
			}
			// Create a new CallbackResponse
			callbackTime := time.Now().UnixMilli()
			newObject := Request{
				Vertex:       cp.vertexName,
				Pipeline:     cp.pipelineName,
				ID:           uuid,
				EventTime:    msg.EventTime.UnixMilli(),
				CallbackTime: callbackTime,
				Tags:         msg.Tags,
				// the read message id has the vertex name of the vertex that sent the message
				FromVertex: pair.ReadMessage.ID.VertexName,
			}
			// if the callback URL is not present in the map, create a new slice
			if _, ok = callbackUrlMap[callbackURL]; !ok {
				callbackUrlMap[callbackURL] = make([]Request, 0)
			}

			// Add new CallbackResponse to map, grouped by the Callback URL
			callbackUrlMap[callbackURL] = append(callbackUrlMap[callbackURL], newObject)
		}
	}

	if err := cp.executeCallback(ctx, callbackUrlMap); err != nil {
		return fmt.Errorf("error executing callback: %w", err)
	}

	return nil
}

// SinkVertexCallback groups messages based on their callback URL and sends a POST request for each group
// for sink vertices. If the callbackHeaderKey is set, it writes all the callback requests to the callbackURL.
// In case of failure while writing the url from the headers, it writes all the callback requests to the callbackURL.
func (cp *Publisher) SinkVertexCallback(ctx context.Context, messages []isb.Message) error {
	// Create a map to hold groups of messagePairs for each callback URL
	callbackUrlMap := make(map[string][]Request)

	for _, msg := range messages {
		var callbackURL string

		if cp.opts.callbackHeaderKey == "" {
			callbackURL = cp.opts.callbackURL
		} else {
			// Extract Callback URL from read message headers
			var ok bool
			callbackURL, ok = msg.Headers[cp.opts.callbackHeaderKey]
			if !ok {
				// Return an error if Callback URL is not found in pair headers
				return errors.New(fmt.Sprintf("Callback URL not found in message headers for key: %s", cp.opts.callbackHeaderKey))
			}
		}

		// Extract UUID from pair headers
		uuid, ok := msg.Headers["uuid"]
		if !ok {
			// Return an error if UUID is not found in pair headers
			return errors.New(fmt.Sprintf("UUID not found in message headers"))
		}

		// Create a new CallbackResponse
		callbackTime := time.Now().UnixMilli()
		newObject := Request{
			Vertex:       cp.vertexName,
			Pipeline:     cp.pipelineName,
			ID:           uuid,
			EventTime:    msg.EventTime.UnixMilli(),
			CallbackTime: callbackTime,
			FromVertex:   msg.ID.VertexName,
		}

		// if the callback URL is not present in the map, create a new slice
		if _, ok = callbackUrlMap[callbackURL]; !ok {
			callbackUrlMap[callbackURL] = make([]Request, 0)
		}

		// Add new CallbackResponse to map, grouped by the Callback URL
		callbackUrlMap[callbackURL] = append(callbackUrlMap[callbackURL], newObject)
	}

	if err := cp.executeCallback(ctx, callbackUrlMap); err != nil {
		return fmt.Errorf("error executing callback: %w", err)
	}

	return nil
}

// executeCallback sends POST requests to the provided callback URLs with the corresponding request bodies.
// It returns an error if any of the requests fail.
func (cp *Publisher) executeCallback(ctx context.Context, callbackUrlMap map[string][]Request) error {
	var failedRequests []Request

	for url, requests := range callbackUrlMap {
		err := cp.sendRequest(ctx, url, requests)
		if err != nil {
			cp.opts.logger.Errorw("Failed to send request, will try writing to the callback URL",
				zap.String("url", url),
				zap.Error(err),
			)
			failedRequests = append(failedRequests, requests...)
			continue
		}
	}

	if len(failedRequests) > 0 {
		err := cp.sendRequest(ctx, cp.opts.callbackURL, failedRequests)
		if err != nil {
			cp.opts.logger.Errorw("Failed to send request to fallback URL",
				zap.String("url", cp.opts.callbackURL),
				zap.Error(err),
			)
		}
	}

	return nil
}

// sendRequest sends a POST request to the provided URL with the provided requests.
// It returns an error if the request fails or if the response status is not OK.
func (cp *Publisher) sendRequest(ctx context.Context, url string, requests []Request) error {
	client := cp.GetClient(url)

	body, err := json.Marshal(requests)
	if err != nil {
		return fmt.Errorf("failed to marshal requests: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-OK response status: %s", resp.Status)
	}

	_ = resp.Body.Close()
	return nil
}

// GetClient returns a client for the given URL from the cache
// If the client is not in the cache, a new one is created
func (cp *Publisher) GetClient(url string) *http.Client {
	// Check if the client is in the cache
	if client, ok := cp.clientsCache.Get(url); ok {
		return client
	}

	// If the client is not in the cache, create a new one
	client := &http.Client{
		Timeout: cp.opts.httpTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // Set InsecureSkipVerify to true
			},
		},
	}

	// Add the new client to the cache
	cp.clientsCache.Add(url, client)

	return client
}

// Close closes all clients in the cache
func (cp *Publisher) Close() {
	for _, client := range cp.clientsCache.Values() {
		client.CloseIdleConnections()
	}
}
