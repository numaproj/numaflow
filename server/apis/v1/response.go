package v1

type NumaflowAPIResponse struct {
	// ErrMsg provides more detailed error information. If API call succeeds, the ErrMsg is nil.
	ErrMsg *string `json:"errMsg,omitempty"`
	// Data is the response body.
	Data interface{} `json:"data"`
}

// NewNumaflowAPIResponse creates a new NumaflowAPIResponse.
func NewNumaflowAPIResponse(errMsg *string, data interface{}) NumaflowAPIResponse {
	return NumaflowAPIResponse{
		ErrMsg: errMsg,
		Data:   data,
	}
}
