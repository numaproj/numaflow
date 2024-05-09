package callback

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
)

// Publisher provides methods to send callback messages.
type Publisher struct {
	CallbackObj  *v1alpha1.Callback
	VertexName   string
	PipelineName string
}

// ResponseObject represents the structure of a response object
type ResponseObject struct {
	Vertex       string `json:"vertex"`
	Pipeline     string `json:"pipeline"`
	UUID         string `json:"uuid"`
	EventTime    int64  `json:"event_time"`
	CallbackTime int64  `json:"callback_time"`
	// dropped and index
}

// DoCallBack groups messages based on their callback URL and sends a POST request for each group
func (cp *Publisher) DoCallBack(messages []isb.Message) error {
	// Create a map to hold groups of messages for each callback URL
	callbackUrlMap := make(map[string][]ResponseObject)

	// Iterate through each message
	for _, message := range messages {
		// Extract Callback URL from message headers
		callbackURL, ok := message.Header.Headers[cp.CallbackObj.CallbackURLHeaderKey]
		if !ok {
			// Return an error if Callback URL is not found in message headers
			return errors.New(fmt.Sprintf("Callback URL not found in message headers for key: %s", cp.CallbackObj.CallbackURLHeaderKey))
		}

		// Extract UUID from message headers
		uuid, ok := message.Header.Headers["uuid"]
		if !ok {
			// Return an error if UUID is not found in message headers
			return errors.New(fmt.Sprintf("UUID not found in message headers"))
		}

		// Create a new ResponseObject
		callbackTime := time.Now().UnixMilli()
		newObject := ResponseObject{
			Vertex:       cp.VertexName,
			Pipeline:     cp.PipelineName,
			UUID:         uuid,
			EventTime:    message.EventTime.UnixMilli(),
			CallbackTime: callbackTime,
		}

		// if the callback URL is not present in the map, create a new slice
		if _, ok = callbackUrlMap[callbackURL]; !ok {
			callbackUrlMap[callbackURL] = make([]ResponseObject, 0)
		}

		// Add new ResponseObject to map, grouped by the Callback URL
		callbackUrlMap[callbackURL] = append(callbackUrlMap[callbackURL], newObject)
	}

	// Send a POST request for each Callback URL group
	for url, responseObject := range callbackUrlMap {
		// Convert the ResponseObjects to JSON
		body, err := json.Marshal(responseObject)
		if err != nil {
			return err
		}

		// Send the POST request with the ResponseObjects as the body
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
		if err != nil || resp.StatusCode != http.StatusOK {
			return errors.New(fmt.Sprintf("Failed to send to callback URL %s with err: %v", url, err))
		}
	}

	return nil
}
