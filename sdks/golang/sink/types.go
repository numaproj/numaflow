package sink

// Message is used to wrap the message written to the user defined sink
type Message struct {
	// Each message has an ID
	ID      string `json:"id"`
	Payload []byte `json:"payload"`
}

// Response is the processing result of each message
type Response struct {
	// ID corresponds the ID in the message.
	ID string `json:"id"`
	// Successful or not. If it's false, "err" is expected to be present.
	Success bool `json:"success"`
	// Err represents the error message when "success" is false.
	Err string `json:"err,omitempty"`
}

type Responses []Response

// ResponsesBuilder returns an empty instance of Responses
func ResponsesBuilder() Responses {
	return Responses{}
}

// Append appends a response
func (r Responses) Append(rep Response) Responses {
	r = append(r, rep)
	return r
}

// Items returns the response list
func (r Responses) Items() []Response {
	return r
}

func ResponseOK(id string) Response {
	return Response{ID: id, Success: true}
}

func ResponseFailure(id, errMsg string) Response {
	return Response{ID: id, Success: false, Err: errMsg}
}
