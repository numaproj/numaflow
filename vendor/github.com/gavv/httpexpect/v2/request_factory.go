package httpexpect

import (
	"io"
	"net/http"
)

// DefaultRequestFactory is the default RequestFactory implementation which just
// calls http.NewRequest.
type DefaultRequestFactory struct{}

// NewRequest implements RequestFactory.NewRequest.
func (DefaultRequestFactory) NewRequest(
	method, url string, body io.Reader,
) (*http.Request, error) {
	return http.NewRequest(method, url, body)
}
