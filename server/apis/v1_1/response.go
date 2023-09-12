package v1_1

// StatusCode is the status of the API call.
type StatusCode string

// TODO: finalize on the types of status code
const (
	Success StatusCode = "success"
	Failure StatusCode = "failure"
)

type NumaflowAPIResponse struct {
	// StatusCode indicates the status of the API call. It can be success or any other error code.
	StatusCode StatusCode `json:"statusCode"`
	// ErrMessage provides more detailed error information. If API call succeeds, the ErrMessage is nil.
	ErrMessage *string `json:"errMessage"`
	// Data is the response body.
	Data interface{} `json:"data"`
}

// NewNumaflowAPIResponse creates a new NumaflowAPIResponse.
func NewNumaflowAPIResponse(code StatusCode, errMessage *string, data interface{}) NumaflowAPIResponse {
	return NumaflowAPIResponse{
		StatusCode: code,
		ErrMessage: errMessage,
		Data:       data,
	}
}
