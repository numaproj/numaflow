package v1_1

type K8sEventsResponse struct {
	TimeStamp string `json:"timestamp"`
	Object    string `json:"object"`
	Reason    string `json:"reason"`
	Message   string `json:"message"`
}

// NewK8sEventsResponse creates a new K8sEventsResponse object with the given inputs.
func NewK8sEventsResponse(timestamp, object, reason, message string) K8sEventsResponse {
	return K8sEventsResponse{
		TimeStamp: timestamp,
		Object:    object,
		Reason:    reason,
		Message:   message,
	}
}
