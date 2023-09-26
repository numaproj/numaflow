package v1_1

type K8sEventsObject struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type K8sEventsResponse struct {
	TimeStamp string          `json:"timestamp"`
	Object    K8sEventsObject `json:"object"`
	Reason    string          `json:"reason"`
	Message   string          `json:"message"`
}

// NewK8sEventsResponse creates a new K8sEventsResponse object with the given inputs.
func NewK8sEventsResponse(timestamp, objectKind, objectName, reason, message string) K8sEventsResponse {

	return K8sEventsResponse{
		TimeStamp: timestamp,
		Object: K8sEventsObject{
			Kind: objectKind,
			Name: objectName,
		},
		Reason:  reason,
		Message: message,
	}
}
