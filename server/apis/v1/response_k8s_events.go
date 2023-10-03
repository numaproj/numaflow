package v1

import "fmt"

type K8sEventsResponse struct {
	TimeStamp int64  `json:"timestamp"`
	Type      string `json:"type"`
	Object    string `json:"object"`
	Reason    string `json:"reason"`
	Message   string `json:"message"`
}

// NewK8sEventsResponse creates a new K8sEventsResponse object with the given inputs.
func NewK8sEventsResponse(timestamp int64, eventType, objectKind, objectName, reason, message string) K8sEventsResponse {

	return K8sEventsResponse{
		TimeStamp: timestamp,
		Type:      eventType,
		Object:    fmt.Sprintf("%s/%s", objectKind, objectName),
		Reason:    reason,
		Message:   message,
	}
}
