package httpexpect

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ajg/form"
	"github.com/gorilla/websocket"
)

// Response provides methods to inspect attached http.Response object.
type Response struct {
	noCopy noCopy
	config Config
	chain  *chain

	httpResp  *http.Response
	websocket *websocket.Conn
	rtt       *time.Duration

	content      []byte
	contentState contentState

	cookies []*http.Cookie
}

type contentState int

const (
	contentPending contentState = iota
	contentRetreived
	contentFailed
)

// NewResponse returns a new Response instance.
//
// If reporter is nil, the function panics.
// If response is nil, failure is reported.
//
// If rtt is given, it defines response round-trip time to be reported
// by response.RoundTripTime().
func NewResponse(
	reporter Reporter, response *http.Response, rtt ...time.Duration,
) *Response {
	config := Config{Reporter: reporter}
	config = config.withDefaults()

	return newResponse(responseOpts{
		config:   config,
		chain:    newChainWithConfig("Response()", config),
		httpResp: response,
		rtt:      rtt,
	})
}

// NewResponse returns a new Response instance with config.
//
// Requirements for config are same as for WithConfig function.
// If response is nil, failure is reported.
//
// If rtt is given, it defines response round-trip time to be reported
// by response.RoundTripTime().
func NewResponseC(
	config Config, response *http.Response, rtt ...time.Duration,
) *Response {
	config = config.withDefaults()

	return newResponse(responseOpts{
		config:   config,
		chain:    newChainWithConfig("Response()", config),
		httpResp: response,
		rtt:      rtt,
	})
}

type responseOpts struct {
	config    Config
	chain     *chain
	httpResp  *http.Response
	websocket *websocket.Conn
	rtt       []time.Duration
}

func newResponse(opts responseOpts) *Response {
	opts.config.validate()

	r := &Response{
		config:       opts.config,
		chain:        opts.chain.clone(),
		contentState: contentPending,
	}

	opChain := r.chain.enter("")
	defer opChain.leave()

	if len(opts.rtt) > 1 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple rtt arguments"),
			},
		})
		return r
	}

	if len(opts.rtt) > 0 {
		rttCopy := opts.rtt[0]
		r.rtt = &rttCopy
	}

	if opts.httpResp == nil {
		opChain.fail(AssertionFailure{
			Type:   AssertNotNil,
			Actual: &AssertionValue{opts.httpResp},
			Errors: []error{
				errors.New("expected: non-nil response"),
			},
		})
		return r
	}

	r.httpResp = opts.httpResp

	if r.httpResp.Body != nil && r.httpResp.Body != http.NoBody {
		if _, ok := r.httpResp.Body.(*bodyWrapper); !ok {
			respCopy := *r.httpResp
			r.httpResp = &respCopy
			r.httpResp.Body = newBodyWrapper(r.httpResp.Body, nil)
		}
	}

	r.websocket = opts.websocket
	r.cookies = r.httpResp.Cookies()

	r.chain.setResponse(r)

	return r
}

func (r *Response) getContent(opChain *chain) ([]byte, bool) {
	switch r.contentState {
	case contentRetreived:
		return r.content, true

	case contentFailed:
		return nil, false

	case contentPending:
		break
	}

	resp := r.httpResp

	if resp.Body == nil || resp.Body == http.NoBody {
		return []byte{}, true
	}

	if bw, ok := resp.Body.(*bodyWrapper); ok {
		bw.Rewind()
	}

	content, err := io.ReadAll(resp.Body)

	closeErr := resp.Body.Close()
	if err == nil {
		err = closeErr
	}

	if err != nil {
		opChain.fail(AssertionFailure{
			Type: AssertOperation,
			Errors: []error{
				errors.New("failed to read response body"),
				err,
			},
		})

		r.content = nil
		r.contentState = contentFailed

		return nil, false
	}

	r.content = content
	r.contentState = contentRetreived

	return r.content, true
}

// Raw returns underlying http.Response object.
// This is the value originally passed to NewResponse.
func (r *Response) Raw() *http.Response {
	return r.httpResp
}

// Alias is similar to Value.Alias.
func (r *Response) Alias(name string) *Response {
	opChain := r.chain.enter("Alias(%q)", name)
	defer opChain.leave()

	r.chain.setAlias(name)
	return r
}

// RoundTripTime returns a new Duration instance with response round-trip time.
//
// The returned duration is the time interval starting just before request is
// sent and ending right after response is received (handshake finished for
// WebSocket request), retrieved from a monotonic clock source.
//
// Example:
//
//	resp := NewResponse(t, response, time.Duration(10000000))
//	resp.RoundTripTime().Lt(10 * time.Millisecond)
func (r *Response) RoundTripTime() *Duration {
	opChain := r.chain.enter("RoundTripTime()")
	defer opChain.leave()

	if opChain.failed() {
		return newDuration(opChain, nil)
	}

	return newDuration(opChain, r.rtt)
}

// Deprecated: use RoundTripTime instead.
func (r *Response) Duration() *Number {
	opChain := r.chain.enter("Duration()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, 0)
	}

	if r.rtt == nil {
		return newNumber(opChain, 0)
	}

	return newNumber(opChain, float64(*r.rtt))
}

// Status succeeds if response contains given status code.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.Status(http.StatusOK)
func (r *Response) Status(status int) *Response {
	opChain := r.chain.enter("Status()")
	defer opChain.leave()

	if opChain.failed() {
		return r
	}

	r.checkEqual(opChain, "http status",
		statusCodeText(status), statusCodeText(r.httpResp.StatusCode))

	return r
}

// StatusRange is enum for response status ranges.
type StatusRange int

const (
	// Status1xx defines "Informational" status codes.
	Status1xx StatusRange = 100

	// Status2xx defines "Success" status codes.
	Status2xx StatusRange = 200

	// Status3xx defines "Redirection" status codes.
	Status3xx StatusRange = 300

	// Status4xx defines "Client Error" status codes.
	Status4xx StatusRange = 400

	// Status5xx defines "Server Error" status codes.
	Status5xx StatusRange = 500
)

// StatusRange succeeds if response status belongs to given range.
//
// Supported ranges:
//   - Status1xx - Informational
//   - Status2xx - Success
//   - Status3xx - Redirection
//   - Status4xx - Client Error
//   - Status5xx - Server Error
//
// See https://en.wikipedia.org/wiki/List_of_HTTP_status_codes.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.StatusRange(Status2xx)
func (r *Response) StatusRange(rn StatusRange) *Response {
	opChain := r.chain.enter("StatusRange()")
	defer opChain.leave()

	if opChain.failed() {
		return r
	}

	status := statusCodeText(r.httpResp.StatusCode)

	actual := statusRangeText(r.httpResp.StatusCode)
	expected := statusRangeText(int(rn))

	if actual == "" || actual != expected {
		opChain.fail(AssertionFailure{
			Type:   AssertBelongs,
			Actual: &AssertionValue{status},
			Expected: &AssertionValue{AssertionList{
				statusRangeText(int(rn)),
			}},
			Errors: []error{
				errors.New("expected: http status belongs to given range"),
			},
		})
	}

	return r
}

// StatusList succeeds if response matches with any given status code list
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.StatusList(http.StatusForbidden, http.StatusUnauthorized)
func (r *Response) StatusList(values ...int) *Response {
	opChain := r.chain.enter("StatusList()")
	defer opChain.leave()

	if opChain.failed() {
		return r
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty status list"),
			},
		})
		return r
	}

	var found bool
	for _, v := range values {
		if v == r.httpResp.StatusCode {
			found = true
			break
		}
	}

	if !found {
		opChain.fail(AssertionFailure{
			Type:     AssertBelongs,
			Actual:   &AssertionValue{statusCodeText(r.httpResp.StatusCode)},
			Expected: &AssertionValue{AssertionList(statusListText(values))},
			Errors: []error{
				errors.New("expected: http status belongs to given list"),
			},
		})
	}

	return r
}

func statusCodeText(code int) string {
	if s := http.StatusText(code); s != "" {
		return strconv.Itoa(code) + " " + s
	}
	return strconv.Itoa(code)
}

func statusRangeText(code int) string {
	switch {
	case code >= 100 && code < 200:
		return "1xx Informational"
	case code >= 200 && code < 300:
		return "2xx Success"
	case code >= 300 && code < 400:
		return "3xx Redirection"
	case code >= 400 && code < 500:
		return "4xx Client Error"
	case code >= 500 && code < 600:
		return "5xx Server Error"
	default:
		return ""
	}
}

func statusListText(values []int) []interface{} {
	var statusText []interface{}
	for _, v := range values {
		statusText = append(statusText, statusCodeText(v))
	}
	return statusText
}

// Headers returns a new Object instance with response header map.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.Headers().Value("Content-Type").String().IsEqual("application-json")
func (r *Response) Headers() *Object {
	opChain := r.chain.enter("Headers()")
	defer opChain.leave()

	if opChain.failed() {
		return newObject(opChain, nil)
	}

	var value map[string]interface{}
	value, _ = canonMap(opChain, r.httpResp.Header)

	return newObject(opChain, value)
}

// Header returns a new String instance with given header field.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.Header("Content-Type").IsEqual("application-json")
//	resp.Header("Date").AsDateTime().Le(time.Now())
func (r *Response) Header(header string) *String {
	opChain := r.chain.enter("Header(%q)", header)
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	value := r.httpResp.Header.Get(header)

	return newString(opChain, value)
}

// Cookies returns a new Array instance with all cookie names set by this response.
// Returned Array contains a String value for every cookie name.
//
// Note that this returns only cookies set by Set-Cookie headers of this response.
// It doesn't return session cookies from previous responses, which may be stored
// in a cookie jar.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.Cookies().Contains("session")
func (r *Response) Cookies() *Array {
	opChain := r.chain.enter("Cookies()")
	defer opChain.leave()

	if opChain.failed() {
		return newArray(opChain, nil)
	}

	names := []interface{}{}
	for _, c := range r.cookies {
		names = append(names, c.Name)
	}

	return newArray(opChain, names)
}

// Cookie returns a new Cookie instance with specified cookie from response.
//
// Note that this returns only cookies set by Set-Cookie headers of this response.
// It doesn't return session cookies from previous responses, which may be stored
// in a cookie jar.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.Cookie("session").Domain().IsEqual("example.com")
func (r *Response) Cookie(name string) *Cookie {
	opChain := r.chain.enter("Cookie(%q)", name)
	defer opChain.leave()

	if opChain.failed() {
		return newCookie(opChain, nil)
	}

	var cookie *Cookie

	names := []string{}
	for _, c := range r.cookies {
		if c.Name == name {
			cookie = newCookie(opChain, c)
			break
		}
		names = append(names, c.Name)
	}

	if cookie == nil {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsElement,
			Actual:   &AssertionValue{names},
			Expected: &AssertionValue{name},
			Errors: []error{
				errors.New("expected: response contains cookie with given name"),
			},
		})
		return newCookie(opChain, nil)
	}

	return cookie
}

// Websocket returns Websocket instance for interaction with WebSocket server.
//
// May be called only if the WithWebsocketUpgrade was called on the request.
// That is responsibility of the caller to explicitly disconnect websocket after use.
//
// Example:
//
//	req := NewRequestC(config, "GET", "/path")
//	req.WithWebsocketUpgrade()
//	ws := req.Expect().Websocket()
//	defer ws.Disconnect()
func (r *Response) Websocket() *Websocket {
	opChain := r.chain.enter("Websocket()")
	defer opChain.leave()

	if opChain.failed() {
		return newWebsocket(opChain, r.config, nil)
	}

	if r.websocket == nil {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New(
					"Websocket() requires WithWebsocketUpgrade() to be called on request"),
			},
		})
		return newWebsocket(opChain, r.config, nil)
	}

	return newWebsocket(opChain, r.config, r.websocket)
}

// Body returns a new String instance with response body.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.Body().NotEmpty()
//	resp.Body().Length().IsEqual(100)
func (r *Response) Body() *String {
	opChain := r.chain.enter("Body()")
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	content, ok := r.getContent(opChain)
	if !ok {
		return newString(opChain, "")
	}

	return newString(opChain, string(content))
}

// NoContent succeeds if response contains empty Content-Type header and
// empty body.
func (r *Response) NoContent() *Response {
	opChain := r.chain.enter("NoContent()")
	defer opChain.leave()

	if opChain.failed() {
		return r
	}

	contentType := r.httpResp.Header.Get("Content-Type")
	if !r.checkEqual(opChain, `"Content-Type" header`, "", contentType) {
		return r
	}

	content, ok := r.getContent(opChain)
	if !ok {
		return r
	}
	if !r.checkEqual(opChain, "body", "", string(content)) {
		return r
	}

	return r
}

// HasContentType succeeds if response contains Content-Type header with given
// media type and charset.
//
// If charset is omitted, and mediaType is non-empty, Content-Type header
// should contain empty or utf-8 charset.
//
// If charset is omitted, and mediaType is also empty, Content-Type header
// should contain no charset.
func (r *Response) HasContentType(mediaType string, charset ...string) *Response {
	opChain := r.chain.enter("HasContentType()")
	defer opChain.leave()

	if opChain.failed() {
		return r
	}

	if len(charset) > 1 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple charset arguments"),
			},
		})
		return r
	}

	r.checkContentType(opChain, mediaType, charset...)

	return r
}

// HasContentEncoding succeeds if response has exactly given Content-Encoding list.
// Common values are empty, "gzip", "compress", "deflate", "identity" and "br".
func (r *Response) HasContentEncoding(encoding ...string) *Response {
	opChain := r.chain.enter("HasContentEncoding()")
	defer opChain.leave()

	if opChain.failed() {
		return r
	}

	r.checkEqual(opChain, `"Content-Encoding" header`,
		encoding,
		r.httpResp.Header["Content-Encoding"])

	return r
}

// HasTransferEncoding succeeds if response contains given Transfer-Encoding list.
// Common values are empty, "chunked" and "identity".
func (r *Response) HasTransferEncoding(encoding ...string) *Response {
	opChain := r.chain.enter("HasTransferEncoding()")
	defer opChain.leave()

	if opChain.failed() {
		return r
	}

	r.checkEqual(opChain, `"Transfer-Encoding" header`,
		encoding,
		r.httpResp.TransferEncoding)

	return r
}

// Deprecated: use HasContentType instead.
func (r *Response) ContentType(mediaType string, charset ...string) *Response {
	return r.HasContentType(mediaType, charset...)
}

// Deprecated: use HasContentEncoding instead.
func (r *Response) ContentEncoding(encoding ...string) *Response {
	return r.HasContentEncoding(encoding...)
}

// Deprecated: use HasTransferEncoding instead.
func (r *Response) TransferEncoding(encoding ...string) *Response {
	return r.HasTransferEncoding(encoding...)
}

// ContentOpts define parameters for matching the response content parameters.
type ContentOpts struct {
	// The media type Content-Type part, e.g. "application/json"
	MediaType string
	// The character set Content-Type part, e.g. "utf-8"
	Charset string
}

// Text returns a new String instance with response body.
//
// Text succeeds if response contains "text/plain" Content-Type header
// with empty or "utf-8" charset.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.Text().IsEqual("hello, world!")
//	resp.Text(ContentOpts{
//	  MediaType: "text/plain",
//	}).IsEqual("hello, world!")
func (r *Response) Text(options ...ContentOpts) *String {
	opChain := r.chain.enter("Text()")
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	if len(options) > 1 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple options arguments"),
			},
		})
		return newString(opChain, "")
	}

	if !r.checkContentOptions(opChain, options, "text/plain") {
		return newString(opChain, "")
	}

	content, ok := r.getContent(opChain)
	if !ok {
		return newString(opChain, "")
	}

	return newString(opChain, string(content))
}

// Form returns a new Object instance with form decoded from response body.
//
// Form succeeds if response contains "application/x-www-form-urlencoded"
// Content-Type header and if form may be decoded from response body.
// Decoding is performed using https://github.com/ajg/form.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.Form().Value("foo").IsEqual("bar")
//	resp.Form(ContentOpts{
//	  MediaType: "application/x-www-form-urlencoded",
//	}).Value("foo").IsEqual("bar")
func (r *Response) Form(options ...ContentOpts) *Object {
	opChain := r.chain.enter("Form()")
	defer opChain.leave()

	if opChain.failed() {
		return newObject(opChain, nil)
	}

	if len(options) > 1 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple options arguments"),
			},
		})
		return newObject(opChain, nil)
	}

	object := r.getForm(opChain, options...)

	return newObject(opChain, object)
}

func (r *Response) getForm(
	opChain *chain, options ...ContentOpts,
) map[string]interface{} {
	if !r.checkContentOptions(opChain, options, "application/x-www-form-urlencoded", "") {
		return nil
	}

	content, ok := r.getContent(opChain)
	if !ok {
		return nil
	}

	decoder := form.NewDecoder(bytes.NewReader(content))

	var object map[string]interface{}

	if err := decoder.Decode(&object); err != nil {
		opChain.fail(AssertionFailure{
			Type: AssertValid,
			Actual: &AssertionValue{
				string(content),
			},
			Errors: []error{
				errors.New("failed to decode form"),
				err,
			},
		})
		return nil
	}

	return object
}

// JSON returns a new Value instance with JSON decoded from response body.
//
// JSON succeeds if response contains "application/json" Content-Type header
// with empty or "utf-8" charset and if JSON may be decoded from response body.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.JSON().Array().ConsistsOf("foo", "bar")
//	resp.JSON(ContentOpts{
//	  MediaType: "application/json",
//	}).Array.ConsistsOf("foo", "bar")
func (r *Response) JSON(options ...ContentOpts) *Value {
	opChain := r.chain.enter("JSON()")
	defer opChain.leave()

	if opChain.failed() {
		return newValue(opChain, nil)
	}

	if len(options) > 1 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple options arguments"),
			},
		})
		return newValue(opChain, nil)
	}

	value := r.getJSON(opChain, options...)

	return newValue(opChain, value)
}

func (r *Response) getJSON(opChain *chain, options ...ContentOpts) interface{} {
	if !r.checkContentOptions(opChain, options, "application/json") {
		return nil
	}

	content, ok := r.getContent(opChain)
	if !ok {
		return nil
	}

	var value interface{}

	if err := json.Unmarshal(content, &value); err != nil {
		opChain.fail(AssertionFailure{
			Type: AssertValid,
			Actual: &AssertionValue{
				string(content),
			},
			Errors: []error{
				errors.New("failed to decode json"),
				err,
			},
		})
		return nil
	}

	return value
}

// JSONP returns a new Value instance with JSONP decoded from response body.
//
// JSONP succeeds if response contains "application/javascript" Content-Type
// header with empty or "utf-8" charset and response body of the following form:
//
//	callback(<valid json>);
//
// or:
//
//	callback(<valid json>)
//
// Whitespaces are allowed.
//
// Example:
//
//	resp := NewResponse(t, response)
//	resp.JSONP("myCallback").Array().ConsistsOf("foo", "bar")
//	resp.JSONP("myCallback", ContentOpts{
//	  MediaType: "application/javascript",
//	}).Array.ConsistsOf("foo", "bar")
func (r *Response) JSONP(callback string, options ...ContentOpts) *Value {
	opChain := r.chain.enter("JSONP()")
	defer opChain.leave()

	if opChain.failed() {
		return newValue(opChain, nil)
	}

	if len(options) > 1 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple options arguments"),
			},
		})
		return newValue(opChain, nil)
	}

	value := r.getJSONP(opChain, callback, options...)

	return newValue(opChain, value)
}

var (
	jsonp = regexp.MustCompile(`^\s*([^\s(]+)\s*\((.*)\)\s*;*\s*$`)
)

func (r *Response) getJSONP(
	opChain *chain, callback string, options ...ContentOpts,
) interface{} {
	if !r.checkContentOptions(opChain, options, "application/javascript") {
		return nil
	}

	content, ok := r.getContent(opChain)
	if !ok {
		return nil
	}

	m := jsonp.FindSubmatch(content)

	if len(m) != 3 || string(m[1]) != callback {
		opChain.fail(AssertionFailure{
			Type: AssertValid,
			Actual: &AssertionValue{
				string(content),
			},
			Errors: []error{
				fmt.Errorf(`expected: JSONP body in form of "%s(<valid json>)"`,
					callback),
			},
		})
		return nil
	}

	var value interface{}

	if err := json.Unmarshal(m[2], &value); err != nil {
		opChain.fail(AssertionFailure{
			Type: AssertValid,
			Actual: &AssertionValue{
				string(content),
			},
			Errors: []error{
				errors.New("failed to decode json"),
				err,
			},
		})
		return nil
	}

	return value
}

func (r *Response) checkContentOptions(
	opChain *chain, options []ContentOpts, expectedType string, expectedCharset ...string,
) bool {
	if len(options) != 0 {
		if options[0].MediaType != "" {
			expectedType = options[0].MediaType
		}
		if options[0].Charset != "" {
			expectedCharset = []string{options[0].Charset}
		}
	}
	return r.checkContentType(opChain, expectedType, expectedCharset...)
}

func (r *Response) checkContentType(
	opChain *chain, expectedType string, expectedCharset ...string,
) bool {
	contentType := r.httpResp.Header.Get("Content-Type")

	if expectedType == "" && len(expectedCharset) == 0 {
		if contentType == "" {
			return true
		}
	}

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{contentType},
			Errors: []error{
				errors.New(`invalid "Content-Type" response header`),
				err,
			},
		})
		return false
	}

	if mediaType != expectedType {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{mediaType},
			Expected: &AssertionValue{expectedType},
			Errors: []error{
				errors.New(`unexpected media type in "Content-Type" response header`),
			},
		})
		return false
	}

	charset := params["charset"]

	if len(expectedCharset) == 0 {
		if charset != "" && !strings.EqualFold(charset, "utf-8") {
			opChain.fail(AssertionFailure{
				Type:     AssertBelongs,
				Actual:   &AssertionValue{charset},
				Expected: &AssertionValue{AssertionList{"", "utf-8"}},
				Errors: []error{
					errors.New(`unexpected charset in "Content-Type" response header`),
				},
			})
			return false
		}
	} else {
		if !strings.EqualFold(charset, expectedCharset[0]) {
			opChain.fail(AssertionFailure{
				Type:     AssertEqual,
				Actual:   &AssertionValue{charset},
				Expected: &AssertionValue{expectedCharset[0]},
				Errors: []error{
					errors.New(`unexpected charset in "Content-Type" response header`),
				},
			})
			return false
		}
	}

	return true
}

func (r *Response) checkEqual(
	opChain *chain, what string, expected, actual interface{},
) bool {
	if !reflect.DeepEqual(expected, actual) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{actual},
			Expected: &AssertionValue{expected},
			Errors: []error{
				fmt.Errorf("unexpected %s value", what),
			},
		})
		return false
	}

	return true
}
