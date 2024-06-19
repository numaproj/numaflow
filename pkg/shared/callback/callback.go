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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
)

// Uploader uploads the callback messages to the callback endpoint.
type Uploader struct {
	vertexName   string
	pipelineName string
	clientsCache *lru.Cache[string, *http.Client]
	opts         *Options
}

// NewUploader creates a new callback Uploader.
func NewUploader(ctx context.Context, vertexName string, pipelineName string, opts ...OptionFunc) *Uploader {
	dOpts := DefaultOptions(ctx)
	for _, opt := range opts {
		opt(dOpts)
	}

	clientCache, _ := lru.NewWithEvict[string, *http.Client](dOpts.cacheSize, func(key string, value *http.Client) {
		// Close the client when it's removed from the cache
		value.CloseIdleConnections()
	})

	return &Uploader{
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
	// CbTime is the time when the callback was made
	CbTime int64 `json:"cb_time"`
	// Tags is the list of tags associated with the message
	Tags []string `json:"tags,omitempty"` // Add 'omitempty' here
	// FromVertex is the name of the vertex from which the message was sent
	FromVertex string `json:"from_vertex"`
}

// NonSinkVertexCallback groups callbacks based on their callback URL and makes a POST request for each callback group
// for non-sink vertices. If the callbackHeaderKey is set, it writes all the callback requests to the callbackURL.
// In case of failure while writing the url from the headers, it writes all the callback requests to the callbackURL.
// The callback URL could change because of the affinity towards colocating the callbacks to the same endpoint server
// that initiated the request (each request could be from a different server in the pool).
func (u *Uploader) NonSinkVertexCallback(ctx context.Context, messagePairs []isb.ReadWriteMessagePair) error {
	// Create a map to hold groups of messagePairs for each callback URL
	callbackUrlMap := make(map[string][]Request)

	// Iterate through each pair
	for _, pair := range messagePairs {

		for _, msg := range pair.WriteMessages {

			// Extract Callback URL from message headers or use the default callback URL
			var callbackURL string
			if cbURL, ok := msg.Headers[u.opts.callbackHeaderKey]; !ok {
				if u.opts.callbackURL == "" {
					return fmt.Errorf("callback URL not found in headers and default callback URL is not set")
				}
				callbackURL = u.opts.callbackURL
			} else {
				callbackURL = cbURL
			}

			// Extract UUID from pair headers
			uuid, ok := msg.Headers[dfv1.KeyMetaID]
			if !ok {
				// Return an error if UUID is not found in pair headers
				return errors.New("ID not found in message headers")
			}

			// Create a new CallbackResponse
			callbackTime := time.Now().UnixMilli()
			newObject := Request{
				Vertex:   u.vertexName,
				Pipeline: u.pipelineName,
				ID:       uuid,
				CbTime:   callbackTime,
				Tags:     msg.Tags,
				// the read message id has the vertex name of the vertex that sent the message
				FromVertex: pair.ReadMessage.ID.VertexName,
			}
			// if the callback URL is not present in the map, create a new slice
			if _, ok = callbackUrlMap[callbackURL]; !ok {
				callbackUrlMap[callbackURL] = make([]Request, 0)
			}

			// append the new callback response to the map grouped by the callback URL
			callbackUrlMap[callbackURL] = append(callbackUrlMap[callbackURL], newObject)
		}
	}

	if err := u.executeCallback(ctx, callbackUrlMap); err != nil {
		return fmt.Errorf("error executing callback: %w", err)
	}

	return nil
}

// SinkVertexCallback groups callbacks based on their callback URL present in the headers and sends a POST request for
// each callback group for sink vertices. If the callback header is not set, it writes all the callback requests to the callbackURL.
// In case of failure while writing the url from the headers, it writes all the callback requests to the callbackURL.
func (u *Uploader) SinkVertexCallback(ctx context.Context, messages []isb.Message) error {
	// Create a map to hold set of callback requests for each callback URL
	callbackUrlMap := make(map[string][]Request)

	for _, msg := range messages {
		// Extract Callback URL from message headers or use the default callback URL
		var callbackURL string
		if cbURL, ok := msg.Headers[u.opts.callbackHeaderKey]; !ok {
			if u.opts.callbackURL == "" {
				return fmt.Errorf("callback URL not found in headers and default callback URL is not set")
			}
			callbackURL = u.opts.callbackURL
		} else {
			callbackURL = cbURL
		}

		// Extract UUID from message headers
		uuid, ok := msg.Headers[dfv1.KeyMetaID]
		if !ok {
			// Return an error if UUID is not found in pair headers
			return errors.New("ID not found in message headers")
		}

		// Create a new CallbackResponse
		// for sink vertex since the message is not paired(terminal vertex). The from_vertex can be
		// extracted from the message itself unlike the non-sink vertex where it is extracted from the
		// read message.
		callbackTime := time.Now().UnixMilli()
		newObject := Request{
			Vertex:     u.vertexName,
			Pipeline:   u.pipelineName,
			ID:         uuid,
			CbTime:     callbackTime,
			FromVertex: msg.ID.VertexName,
		}

		// if the callback URL is not present in the map, create a new slice
		if _, ok = callbackUrlMap[callbackURL]; !ok {
			callbackUrlMap[callbackURL] = make([]Request, 0)
		}

		// Add new CallbackResponse to map, grouped by the Callback URL
		callbackUrlMap[callbackURL] = append(callbackUrlMap[callbackURL], newObject)
	}

	if err := u.executeCallback(ctx, callbackUrlMap); err != nil {
		return fmt.Errorf("error executing callback: %w", err)
	}

	return nil
}

// executeCallback sends POST requests to the provided callback URLs with the corresponding request bodies.
// It returns an error if any of the requests fail.
func (u *Uploader) executeCallback(ctx context.Context, callbackUrlMap map[string][]Request) error {
	var failedRequests []Request

	for url, requests := range callbackUrlMap {
		err := u.sendRequest(ctx, url, requests)
		if err != nil {
			u.opts.logger.Errorw("Failed to send request, will try writing to the callback URL",
				zap.String("url", url),
				zap.Error(err),
			)
			failedRequests = append(failedRequests, requests...)
		}
	}

	if len(failedRequests) > 0 && u.opts.callbackURL != "" {
		err := u.sendRequest(ctx, u.opts.callbackURL, failedRequests)
		if err != nil {
			u.opts.logger.Errorw("Failed to send request to callback URL, skipping the callback requests",
				zap.String("url", u.opts.callbackURL),
				zap.Error(err),
			)
		}
	}

	return nil
}

// sendRequest sends a POST request to the provided URL with the provided requests.
// It returns an error if the request fails or if the response status is not OK.
func (u *Uploader) sendRequest(ctx context.Context, url string, requests []Request) error {
	// get a cached client
	client := u.GetClient(url)

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

	defer resp.Body.Close()

	if resp.StatusCode > 299 {
		return fmt.Errorf("received non-OK response status: %s", resp.Status)
	}

	return nil
}

// GetClient returns a client for the given URL from the cache
// If the client is not in the cache, a new one is created.
func (u *Uploader) GetClient(url string) *http.Client {
	// Check if the client is in the cache
	if client, ok := u.clientsCache.Get(url); ok {
		return client
	}

	// If the client is not in the cache, create a new one
	client := &http.Client{
		Timeout: u.opts.httpTimeout,
		Transport: &http.Transport{
			// we don't need authentication for the callback, since we expect it to be a local service.
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	// Add the new client to the cache
	u.clientsCache.Add(url, client)

	return client
}

// Close closes all clients in the cache
func (u *Uploader) Close() {
	// clear the cache, which will call the onEvicted method for each client
	u.clientsCache.Purge()
}
