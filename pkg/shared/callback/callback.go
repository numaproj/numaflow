package callback

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	utils "github.com/numaproj/numaflow/pkg/shared/util"
)

// Publisher is the callback publisher which sends POST requests to callback URLs.
type Publisher struct {
	CallbackObj  *v1alpha1.Callback
	VertexName   string
	PipelineName string
	ClientCache  *lru.Cache[string, *http.Client]
	opts         *Options
}

// NewPublisher creates a new callback publisher
func NewPublisher(callbackObj *v1alpha1.Callback, vertexName string, pipelineName string, opts ...OptionFunc) *Publisher {
	dOpts := DefaultOptions()
	for _, opt := range opts {
		opt(dOpts)
	}

	clientCache, _ := lru.NewWithEvict[string, *http.Client](dOpts.LRUCacheSize, func(key string, value *http.Client) {
		// Close the client when it's removed from the cache
		value.CloseIdleConnections()
	})

	return &Publisher{
		CallbackObj:  callbackObj,
		VertexName:   vertexName,
		PipelineName: pipelineName,
		ClientCache:  clientCache,
		opts:         dOpts,
	}
}

// ResponseObject represents the structure of a response object
type ResponseObject struct {
	// Vertex is the name of the vertex
	Vertex string `json:"vertex"`
	// Pipeline is the name of the pipeline
	Pipeline string `json:"pipeline"`
	// UUID is the unique identifier of the message
	UUID string `json:"uuid"`
	// EventTime is the time when the message was created
	EventTime int64 `json:"event_time"`
	// CallbackTime is the time when the callback was made
	CallbackTime int64 `json:"callback_time"`
	// Index is the index of the message in the pair
	Index int `json:"index"`
	// Dropped is true if the message was dropped
	Dropped bool `json:"dropped"`
}

// NonSinkVertexCallback groups messages based on their callback URL and sends a POST request for each group
func (cp *Publisher) NonSinkVertexCallback(messagePairs []isb.ReadWriteMessagePair) error {
	// Create a map to hold groups of messagePairs for each callback URL
	callbackUrlMap := make(map[string][]ResponseObject)

	// Iterate through each pair
	for _, pair := range messagePairs {
		// Extract Callback URL from read message headers
		callbackURL, ok := pair.ReadMessage.Headers[cp.CallbackObj.CallbackURLHeaderKey]
		if !ok {
			// Return an error if Callback URL is not found in pair headers
			return errors.New(fmt.Sprintf("Callback URL not found in message headers for key: %s", cp.CallbackObj.CallbackURLHeaderKey))
		}

		// Extract UUID from pair headers
		uuid, ok := pair.ReadMessage.Headers["uuid"]
		if !ok {
			// Return an error if UUID is not found in pair headers
			return errors.New(fmt.Sprintf("UUID not found in message headers"))
		}

		for index, msg := range pair.WriteMessages {
			// Create a new ResponseObject
			callbackTime := time.Now().UnixMilli()
			newObject := ResponseObject{
				Vertex:       cp.VertexName,
				Pipeline:     cp.PipelineName,
				UUID:         uuid,
				EventTime:    msg.EventTime.UnixMilli(),
				CallbackTime: callbackTime,
				Index:        index + 1,
			}

			if utils.StringSliceContains(msg.Tags, dfv1.MessageTagDrop) {
				newObject.Dropped = true
			}

			// if the callback URL is not present in the map, create a new slice
			if _, ok = callbackUrlMap[callbackURL]; !ok {
				callbackUrlMap[callbackURL] = make([]ResponseObject, 0)
			}

			// Add new ResponseObject to map, grouped by the Callback URL
			callbackUrlMap[callbackURL] = append(callbackUrlMap[callbackURL], newObject)
		}
	}

	if err := cp.executeCallback(callbackUrlMap); err != nil {
		return fmt.Errorf("error executing callback: %w", err)
	}

	return nil
}

func (cp *Publisher) executeCallback(callbackUrlMap map[string][]ResponseObject) error {
	for url, responseObject := range callbackUrlMap {
		// Convert the ResponseObjects to JSON
		body, err := json.Marshal(responseObject)
		if err != nil {
			return err
		}

		// Get the client for this URL
		client := cp.GetClient(url)

		// Send the POST request with the ResponseObjects as the body
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			return err
		}

		_ = resp.Body.Close()
	}
	return nil
}

func (cp *Publisher) SinkVertexCallback(messages []isb.Message) error {
	// Create a map to hold groups of messagePairs for each callback URL
	callbackUrlMap := make(map[string][]ResponseObject)

	for index, msg := range messages {
		// Extract Callback URL from read message headers
		callbackURL, ok := msg.Headers[cp.CallbackObj.CallbackURLHeaderKey]
		if !ok {
			// Return an error if Callback URL is not found in pair headers
			return errors.New(fmt.Sprintf("Callback URL not found in message headers for key: %s", cp.CallbackObj.CallbackURLHeaderKey))
		}

		// Extract UUID from pair headers
		uuid, ok := msg.Headers["uuid"]
		if !ok {
			// Return an error if UUID is not found in pair headers
			return errors.New(fmt.Sprintf("UUID not found in message headers"))
		}

		// Create a new ResponseObject
		callbackTime := time.Now().UnixMilli()
		newObject := ResponseObject{
			Vertex:       cp.VertexName,
			Pipeline:     cp.PipelineName,
			UUID:         uuid,
			EventTime:    msg.EventTime.UnixMilli(),
			CallbackTime: callbackTime,
			Index:        index + 1,
		}

		// if the callback URL is not present in the map, create a new slice
		if _, ok = callbackUrlMap[callbackURL]; !ok {
			callbackUrlMap[callbackURL] = make([]ResponseObject, 0)
		}

		// Add new ResponseObject to map, grouped by the Callback URL
		callbackUrlMap[callbackURL] = append(callbackUrlMap[callbackURL], newObject)
	}

	if err := cp.executeCallback(callbackUrlMap); err != nil {
		return fmt.Errorf("error executing callback: %w", err)
	}

	return nil
}

// GetClient returns a client for the given URL from the cache
// If the client is not in the cache, a new one is created
func (cp *Publisher) GetClient(url string) *http.Client {
	// Check if the client is in the cache
	if client, ok := cp.ClientCache.Get(url); ok {
		return client
	}

	// If the client is not in the cache, create a new one
	client := &http.Client{
		Timeout: cp.opts.HTTPTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // Set InsecureSkipVerify to true
			},
		},
	}

	// Add the new client to the cache
	cp.ClientCache.Add(url, client)

	return client
}

// Close closes all clients in the cache
func (cp *Publisher) Close() {
	for _, client := range cp.ClientCache.Values() {
		client.CloseIdleConnections()
	}
}
