package v1_1

import "fmt"

type K8sEventsResponse struct {
	TimeStamp int64  `json:"timestamp"`
	Object    string `json:"object"`
	Reason    string `json:"reason"`
	Message   string `json:"message"`
}

// NewK8sEventsResponse creates a new K8sEventsResponse object with the given inputs.
func NewK8sEventsResponse(timestamp int64, objectKind, objectName, reason, message string) K8sEventsResponse {

	return K8sEventsResponse{
		TimeStamp: timestamp,
		Object:    fmt.Sprintf("%s/%s", objectKind, objectName),
		Reason:    reason,
		Message:   message,
	}
}
