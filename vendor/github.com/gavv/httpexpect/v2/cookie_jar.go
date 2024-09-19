package httpexpect

import (
	"net/http"
	"net/http/cookiejar"

	"golang.org/x/net/publicsuffix"
)

// NewCookieJar returns a new http.CookieJar.
//
// Returned jar is implemented in net/http/cookiejar. PublicSuffixList is
// implemented in golang.org/x/net/publicsuffix.
//
// Note that this jar ignores cookies when request url is empty.
func NewCookieJar() http.CookieJar {
	jar, err := cookiejar.New(&cookiejar.Options{
		PublicSuffixList: publicsuffix.List,
	})
	if err != nil {
		panic(err)
	}
	return jar
}

// Deprecated: use NewCookieJar instead.
func NewJar() http.CookieJar {
	return NewCookieJar()
}
