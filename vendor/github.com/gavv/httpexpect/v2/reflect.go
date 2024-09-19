package httpexpect

import (
	"net/http"
	"reflect"
)

func refIsNil(value interface{}) bool {
	defer func() {
		_ = recover()
	}()
	return value == nil || reflect.ValueOf(value).IsNil()
}

func refIsNum(value interface{}) bool {
	defer func() {
		_ = recover()
	}()
	reflect.ValueOf(value).Convert(reflect.TypeOf(float64(0))).Float()
	return true
}

func refIsHTTP(value interface{}) bool {
	switch value.(type) {
	case *http.Client, http.Client,
		*http.Transport, http.Transport,
		*http.Request, http.Request,
		*http.Response, http.Response,
		*http.Header, http.Header,
		*http.Cookie, http.Cookie:
		return true
	default:
		return false
	}
}
