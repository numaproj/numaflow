package v1

// LoginResponse is the response payload for login API.
type LoginResponse struct {
	AuthCodeURL string `json:"AuthCodeURL"`
}

// NewLoginResponse returns a LoginResponse object for the given url.
func NewLoginResponse(url string) LoginResponse {
	return LoginResponse{AuthCodeURL: url}
}
