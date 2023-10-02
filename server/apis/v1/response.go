package v1

type NumaflowAPIResponse struct {
	// ErrMessage provides more detailed error information. If API call succeeds, the ErrMessage is nil.
	ErrMessage *string `json:"errMessage,omitempty"`
	// Data is the response body.
	Data interface{} `json:"data"`
}

// NewNumaflowAPIResponse creates a new NumaflowAPIResponse.
func NewNumaflowAPIResponse(errMessage *string, data interface{}) NumaflowAPIResponse {
	return NumaflowAPIResponse{
		ErrMessage: errMessage,
		Data:       data,
	}
}
