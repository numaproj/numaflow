// Package httpexpect helps with end-to-end HTTP and REST API testing.
//
// # Usage examples
//
// See example directory:
//   - https://pkg.go.dev/github.com/gavv/httpexpect/_examples
//   - https://github.com/gavv/httpexpect/tree/master/_examples
//
// # Communication mode
//
// There are two common ways to test API with httpexpect:
//   - start HTTP server and instruct httpexpect to use HTTP client for communication
//   - don't start server and instruct httpexpect to invoke http handler directly
//
// The second approach works only if the server is a Go module and its handler can
// be imported in tests.
//
// Concrete behaviour is determined by Client implementation passed to Config struct.
// If you're using http.Client, set its Transport field (http.RoundTriper) to one of
// the following:
//  1. default (nil) - use HTTP transport from net/http (you should start server)
//  2. httpexpect.Binder - invoke given http.Handler directly
//  3. httpexpect.FastBinder - invoke given fasthttp.RequestHandler directly
//
// Note that http handler can be usually obtained from http framework you're using.
// E.g., echo framework provides either http.Handler or fasthttp.RequestHandler.
//
// You can also provide your own implementation of RequestFactory (creates http.Request),
// or Client (gets http.Request and returns http.Response).
//
// If you're starting server from tests, it's very handy to use net/http/httptest.
//
// # Value equality
//
// Whenever values are checked for equality in httpexpect, they are converted
// to "canonical form":
//   - structs are converted to map[string]interface{}
//   - type aliases are removed
//   - numeric types are converted to float64
//   - non-nil interfaces pointing to nil slices and maps are replaced with
//     nil interfaces
//
// This is equivalent to subsequently json.Marshal() and json.Unmarshal() the value
// and currently is implemented so.
//
// # Failure handling
//
// When some check fails, failure is reported. If non-fatal failures are used
// (see Reporter interface), execution is continued and instance that was checked
// is marked as failed.
//
// If specific instance is marked as failed, all subsequent checks are ignored
// for this instance and for any child instances retrieved after failure.
//
// Example:
//
//	array := NewArray(NewAssertReporter(t), []interface{}{"foo", 123})
//
//	e0 := array.Value(0)  // success
//	e1 := array.Value(1)  // success
//
//	s0 := e0.String()  // success
//	s1 := e1.String()  // failure; e1 and s1 are marked as failed, e0 and s0 are not
//
//	s0.IsEqual("foo")    // success
//	s1.IsEqual("bar")    // this check is ignored because s1 is marked as failed
//
// # Assertion handling
//
// If you want to be informed about every asserion made, successful or failed, you
// can use AssertionHandler interface.
//
// Default implementation of this interface ignores successful assertions and reports
// failed assertions using Formatter and Reporter objects.
//
// Custom AssertionHandler can handle all assertions (e.g. dump them in JSON format)
// and is free to use or not to use Formatter and Reporter in its sole discretion.
package httpexpect

import (
	"context"
	"io"
	"net/http"

	"github.com/gorilla/websocket"
)

// Expect is a toplevel object that contains user Config and allows
// to construct Request objects.
type Expect struct {
	noCopy   noCopy
	config   Config
	chain    *chain
	builders []func(*Request)
	matchers []func(*Response)
}

// Config contains various settings.
type Config struct {
	// TestName defines the name of the currently running test.
	// May be empty.
	//
	// If non-empty, it will be included in failure report.
	// Normally you set this value to t.Name().
	TestName string

	// BaseURL is a URL to prepended to all requests.
	// May be empty.
	//
	// If non-empty, trailing slash is allowed (but not required) and is appended
	// automatically.
	BaseURL string

	// RequestFactory is used to pass in a custom *http.Request generation func.
	// May be nil.
	//
	// If nil, DefaultRequestFactory is used, which just calls http.NewRequest.
	//
	// You can use DefaultRequestFactory, or provide custom implementation.
	// Useful for Google App Engine testing for example.
	RequestFactory RequestFactory

	// Client is used to send http.Request and receive http.Response.
	// May be nil.
	//
	// If nil, set to a default client with a non-nil Jar:
	//  &http.Client{
	//      Jar: httpexpect.NewCookieJar(),
	//  }
	//
	// You can use http.DefaultClient or your own http.Client, or provide
	// custom implementation.
	Client Client

	// WebsocketDialer is used to establish websocket.Conn and receive http.Response
	// of handshake result.
	// May be nil.
	//
	// If nil, set to a default dialer:
	//  &websocket.Dialer{}
	//
	// You can use websocket.DefaultDialer or websocket.Dialer, or provide
	// custom implementation.
	WebsocketDialer WebsocketDialer

	// Context is passed to all requests. It is typically used for request cancellation,
	// either explicit or after a time-out.
	// May be nil.
	//
	// You can use the Request.WithContext for per-request context and Request.WithTimeout
	// for per-request timeout.
	Context context.Context

	// Reporter is used to report formatted failure messages.
	// Should NOT be nil, unless custom AssertionHandler is used.
	//
	// Config.Reporter is used by DefaultAssertionHandler, which is automatically
	// constructed when AssertionHandler is nil.
	//
	// You can use:
	//  - AssertReporter / RequireReporter
	//    (non-fatal / fatal failures using testify package)
	//  - testing.T / FatalReporter
	//    (non-fatal / fatal failures using standard testing package)
	//  - PanicReporter
	//    (failures that panic to be used in multithreaded tests)
	//  - custom implementation
	Reporter Reporter

	// Formatter is used to format success and failure messages.
	// May be nil.
	//
	// If nil, DefaultFormatter is used.
	//
	// Config.Formatter is used by DefaultAssertionHandler, which is automatically
	// constructed when AssertionHandler is nil.
	//
	// Usually you don't need custom formatter. Implementing one is a
	// relatively big task.
	Formatter Formatter

	// AssertionHandler handles successful and failed assertions.
	// May be nil.
	//
	// Every time an assertion is made, AssertionHandler is invoked with detailed
	// info about the assertion. On failure, AssertionHandler is responsible to
	// format error and report it to the test suite.
	//
	// If AssertionHandler is nil, DefaultAssertionHandler is constructed, with
	// Formatter set to Config.Formatter, Reporter set to Config.Reporter, and
	// Logger set to nil. DefaultAssertionHandler will just delegate formatting
	// and reporting to Formatter and Reporter.
	//
	// If you're happy with DefaultAssertionHandler, but want to enable logging
	// of successful assertions and non-fatal failures, you can manually construct
	// DefaultAssertionHandler and set its Logger field to a non-nil value.
	//
	// Usually you don't need custom AssertionHandler and it's enough just to
	// set Reporter. Use AssertionHandler for more precise control of reports.
	AssertionHandler AssertionHandler

	// Printers are used to print requests and responses.
	// May be nil.
	//
	// If printer implements WebsocketPrinter interface, it will be also used
	// to print Websocket messages.
	//
	// You can use CompactPrinter, DebugPrinter, CurlPrinter, or provide
	// custom implementation.
	//
	// You can also use builtin printers with alternative Logger if you're happy
	// with their format, but want to send logs somewhere else than *testing.T.
	Printers []Printer

	// Environment provides a container for arbitrary data shared between tests.
	// May be nil.
	//
	// Environment is not used by httpexpect itself, but can be used by tests to
	// store and load arbitrary values. Tests can access Environment via
	// Expect.Env(). It is also accessible in AssertionHandler via AssertionContext.
	//
	// If Environment is nil, a new empty environment is automatically created
	// when Expect instance is constructed.
	Environment *Environment
}

func (config Config) withDefaults() Config {
	if config.RequestFactory == nil {
		config.RequestFactory = DefaultRequestFactory{}
	}

	if config.Client == nil {
		config.Client = &http.Client{
			Jar: NewCookieJar(),
		}
	}

	if config.WebsocketDialer == nil {
		config.WebsocketDialer = &websocket.Dialer{}
	}

	if config.AssertionHandler == nil {
		if config.Formatter == nil {
			config.Formatter = &DefaultFormatter{}
		}

		if config.Reporter == nil {
			panic("either Config.Reporter or Config.AssertionHandler should be non-nil")
		}

		config.AssertionHandler = &DefaultAssertionHandler{
			Formatter: config.Formatter,
			Reporter:  config.Reporter,
		}
	}

	return config
}

func (config *Config) validate() {
	if config.RequestFactory == nil {
		panic("Config.RequestFactory is nil")
	}

	if config.Client == nil {
		panic("Config.Client is nil")
	}

	if config.AssertionHandler == nil {
		panic("Config.AssertionHandler is nil")
	}

	if handler, ok := config.AssertionHandler.(*DefaultAssertionHandler); ok {
		if handler.Formatter == nil {
			panic("DefaultAssertionHandler.Formatter is nil")
		}

		if handler.Reporter == nil {
			panic("DefaultAssertionHandler.Reporter is nil")
		}
	}
}

// RequestFactory is used to create all http.Request objects.
// aetest.Instance from the Google App Engine implements this interface.
type RequestFactory interface {
	NewRequest(method, url string, body io.Reader) (*http.Request, error)
}

// RequestFactoryFunc is an adapter that allows a function
// to be used as the RequestFactory
//
// Example:
//
//	e := httpexpect.WithConfig(httpexpect.Config{
//		RequestFactory: httpextect.RequestFactoryFunc(
//			func(method string, url string, body io.Reader) (*http.Request, error) {
//				// factory code here
//			}),
//	})
type RequestFactoryFunc func(
	method string, url string, body io.Reader,
) (*http.Request, error)

func (f RequestFactoryFunc) NewRequest(
	method string, url string, body io.Reader,
) (*http.Request, error) {
	return f(method, url, body)
}

// Client is used to send http.Request and receive http.Response.
// http.Client implements this interface.
//
// Binder and FastBinder may be used to obtain this interface implementation.
//
// Example:
//
//	httpBinderClient := &http.Client{
//	  Transport: httpexpect.NewBinder(HTTPHandler),
//	}
//	fastBinderClient := &http.Client{
//	  Transport: httpexpect.NewFastBinder(FastHTTPHandler),
//	}
type Client interface {
	// Do sends request and returns response.
	Do(*http.Request) (*http.Response, error)
}

// ClientFunc is an adapter that allows a function to be used as the Client
//
// Example:
//
//	e := httpexpect.WithConfig(httpexpect.Config{
//		Client: httpextect.ClientFunc(
//			func(req *http.Request) (*http.Response, error) {
//				// client code here
//			}),
//	})
type ClientFunc func(req *http.Request) (*http.Response, error)

func (f ClientFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

// WebsocketDialer is used to establish websocket.Conn and receive http.Response
// of handshake result.
// websocket.Dialer implements this interface.
//
// NewWebsocketDialer and NewFastWebsocketDialer may be used to obtain this
// interface implementation.
//
// Example:
//
//	e := httpexpect.WithConfig(httpexpect.Config{
//		WebsocketDialer: httpexpect.NewWebsocketDialer(myHandler),
//	})
type WebsocketDialer interface {
	// Dial establishes new Websocket connection and returns response
	// of handshake result.
	Dial(url string, reqH http.Header) (*websocket.Conn, *http.Response, error)
}

// WebsocketDialerFunc is an adapter that allows a function
// to be used as the WebsocketDialer
//
// Example:
//
//	e := httpexpect.WithConfig(httpexpect.Config{
//		WebsocketDialer: httpextect.WebsocketDialerFunc(
//			func(url string, reqH http.Header) (*websocket.Conn, *http.Response, error) {
//				// dialer code here
//			}),
//	})
type WebsocketDialerFunc func(
	url string, reqH http.Header,
) (*websocket.Conn, *http.Response, error)

func (f WebsocketDialerFunc) Dial(
	url string, reqH http.Header,
) (*websocket.Conn, *http.Response, error) {
	return f(url, reqH)
}

// Reporter is used to report failures.
// *testing.T, FatalReporter, AssertReporter, RequireReporter, PanicReporter implement it.
type Reporter interface {
	// Errorf reports failure.
	// Allowed to return normally or terminate test using t.FailNow().
	Errorf(message string, args ...interface{})
}

// ReporterFunc is an adapter that allows a function to be used as the Reporter
//
// Example:
//
//	e := httpexpect.WithConfig(httpexpect.Config{
//		Reporter: httpextect.ReporterFunc(
//			func(message string, args ...interface{}) {
//				// reporter code here
//			}),
//	})
type ReporterFunc func(message string, args ...interface{})

func (f ReporterFunc) Errorf(message string, args ...interface{}) {
	f(message, args)
}

// Logger is used as output backend for Printer.
// *testing.T implements this interface.
type Logger interface {
	// Logf writes message to test log.
	Logf(fmt string, args ...interface{})
}

// LoggerFunc is an adapter that allows a function to be used as the Logger
//
// Example:
//
//	e := httpexpect.WithConfig(httpexpect.Config{
//		Printers: []httpexpect.Printer{
//			httpexpect.NewCompactPrinter(
//				httpextect.LoggerFunc(
//					func(fmt string, args ...interface{}) {
//						// logger code here
//					})),
//		},
//	})
type LoggerFunc func(fmt string, args ...interface{})

func (f LoggerFunc) Logf(fmt string, args ...interface{}) {
	f(fmt, args)
}

// TestingTB is a subset of testing.TB interface used by httpexpect.
// You can use *testing.T or pass custom implementation.
type TestingTB interface {
	Reporter
	Logger
	Name() string // Returns current test name.
}

// Deprecated: use TestingTB instead.
type LoggerReporter interface {
	Logger
	Reporter
}

// Deprecated: use Default instead.
func New(t LoggerReporter, baseURL string) *Expect {
	return WithConfig(Config{
		BaseURL:  baseURL,
		Reporter: NewAssertReporter(t),
		Printers: []Printer{
			NewCompactPrinter(t),
		},
	})
}

// Default returns a new Expect instance with default config.
//
// t is usually *testing.T, but can be any matching implementation.
//
// baseURL specifies URL to be prepended to all requests. May be empty. If non-empty,
// trailing slash is allowed (but not required) and is appended automatically.
//
// Default is a shorthand for WithConfig. It uses:
//   - baseURL for Config.BaseURL
//   - t.Name() for Config.TestName
//   - NewAssertReporter(t) for Config.Reporter
//   - NewCompactPrinter(t) for Config.Printers
//
// Example:
//
//	func TestSomething(t *testing.T) {
//		e := httpexpect.Default(t, "http://example.com/")
//
//		e.GET("/path").
//			Expect().
//			Status(http.StatusOK)
//	}
func Default(t TestingTB, baseURL string) *Expect {
	return WithConfig(Config{
		TestName: t.Name(),
		BaseURL:  baseURL,
		Reporter: NewAssertReporter(t),
		Printers: []Printer{
			NewCompactPrinter(t),
		},
	})
}

// WithConfig returns a new Expect instance with custom config.
//
// Either Reporter or AssertionHandler should not be nil,
// otherwise the function panics.
//
// Example:
//
//	func TestSomething(t *testing.T) {
//		e := httpexpect.WithConfig(httpexpect.Config{
//			TestName: t.Name(),
//			BaseURL:  "http://example.com/",
//			Client:   &http.Client{
//				Transport: httpexpect.NewBinder(myHandler()),
//				Jar:       httpexpect.NewCookieJar(),
//			},
//			Reporter: httpexpect.NewAssertReporter(t),
//			Printers: []httpexpect.Printer{
//				httpexpect.NewCurlPrinter(t),
//				httpexpect.NewDebugPrinter(t, true)
//			},
//		})
//
//		e.GET("/path").
//			Expect().
//			Status(http.StatusOK)
//	}
func WithConfig(config Config) *Expect {
	config = config.withDefaults()

	config.validate()

	return &Expect{
		chain:  newChainWithConfig("", config),
		config: config,
	}
}

// Env returns Environment associated with Expect instance.
// Tests can use it to store arbitrary data.
//
// Example:
//
//	e := httpexpect.Default(t, "http://example.com")
//
//	e.Env().Put("key", "value")
//	value := e.Env().GetString("key")
func (e *Expect) Env() *Environment {
	return e.chain.env()
}

func (e *Expect) clone() *Expect {
	return &Expect{
		config:   e.config,
		chain:    e.chain.clone(),
		builders: append(([]func(*Request))(nil), e.builders...),
		matchers: append(([]func(*Response))(nil), e.matchers...),
	}
}

// Builder returns a copy of Expect instance with given builder attached to it.
// Returned copy contains all previously attached builders plus a new one.
// Builders are invoked from Request method, after constructing every new request.
//
// Example:
//
//	e := httpexpect.Default(t, "http://example.com")
//
//	token := e.POST("/login").WithForm(Login{"ford", "betelgeuse7"}).
//		Expect().
//		Status(http.StatusOK).JSON().Object().Value("token").String().Raw()
//
//	auth := e.Builder(func (req *httpexpect.Request) {
//		req.WithHeader("Authorization", "Bearer "+token)
//	})
//
//	auth.GET("/restricted").
//	   Expect().
//	   Status(http.StatusOK)
func (e *Expect) Builder(builder func(*Request)) *Expect {
	ret := e.clone()

	ret.builders = append(ret.builders, builder)
	return ret
}

// Matcher returns a copy of Expect instance with given matcher attached to it.
// Returned copy contains all previously attached matchers plus a new one.
// Matchers are invoked from Request.Expect method, after retrieving a new response.
//
// Example:
//
//	 e := httpexpect.Default(t, "http://example.com")
//
//	 m := e.Matcher(func (resp *httpexpect.Response) {
//		 resp.Header("API-Version").NotEmpty()
//	 })
//
//	 m.GET("/some-path").
//			Expect().
//			Status(http.StatusOK)
//
//	 m.GET("/bad-path").
//			Expect().
//			Status(http.StatusNotFound)
func (e *Expect) Matcher(matcher func(*Response)) *Expect {
	ret := e.clone()

	ret.matchers = append(ret.matchers, matcher)
	return ret
}

// Request returns a new Request instance.
// Arguments are similar to NewRequest.
// After creating request, all builders attached to Expect instance are invoked.
// See Builder.
func (e *Expect) Request(method, path string, pathargs ...interface{}) *Request {
	opChain := e.chain.enter("Request(%q)", method)
	defer opChain.leave()

	req := newRequest(opChain, e.config, method, path, pathargs...)

	for _, builder := range e.builders {
		builder(req)
	}

	for _, matcher := range e.matchers {
		req.WithMatcher(matcher)
	}

	return req
}

// OPTIONS is a shorthand for e.Request("OPTIONS", path, pathargs...).
func (e *Expect) OPTIONS(path string, pathargs ...interface{}) *Request {
	return e.Request(http.MethodOptions, path, pathargs...)
}

// HEAD is a shorthand for e.Request("HEAD", path, pathargs...).
func (e *Expect) HEAD(path string, pathargs ...interface{}) *Request {
	return e.Request(http.MethodHead, path, pathargs...)
}

// GET is a shorthand for e.Request("GET", path, pathargs...).
func (e *Expect) GET(path string, pathargs ...interface{}) *Request {
	return e.Request(http.MethodGet, path, pathargs...)
}

// POST is a shorthand for e.Request("POST", path, pathargs...).
func (e *Expect) POST(path string, pathargs ...interface{}) *Request {
	return e.Request(http.MethodPost, path, pathargs...)
}

// PUT is a shorthand for e.Request("PUT", path, pathargs...).
func (e *Expect) PUT(path string, pathargs ...interface{}) *Request {
	return e.Request(http.MethodPut, path, pathargs...)
}

// PATCH is a shorthand for e.Request("PATCH", path, pathargs...).
func (e *Expect) PATCH(path string, pathargs ...interface{}) *Request {
	return e.Request(http.MethodPatch, path, pathargs...)
}

// DELETE is a shorthand for e.Request("DELETE", path, pathargs...).
func (e *Expect) DELETE(path string, pathargs ...interface{}) *Request {
	return e.Request(http.MethodDelete, path, pathargs...)
}

// Deprecated: use NewValue or NewValueC instead.
func (e *Expect) Value(value interface{}) *Value {
	opChain := e.chain.enter("Value()")
	defer opChain.leave()

	return newValue(opChain, value)
}

// Deprecated: use NewObject or NewObjectC instead.
func (e *Expect) Object(value map[string]interface{}) *Object {
	opChain := e.chain.enter("Object()")
	defer opChain.leave()

	return newObject(opChain, value)
}

// Deprecated: use NewArray or NewArrayC instead.
func (e *Expect) Array(value []interface{}) *Array {
	opChain := e.chain.enter("Array()")
	defer opChain.leave()

	return newArray(opChain, value)
}

// Deprecated: use NewString or NewStringC instead.
func (e *Expect) String(value string) *String {
	opChain := e.chain.enter("String()")
	defer opChain.leave()

	return newString(opChain, value)
}

// Deprecated: use NewNumber or NewNumberC instead.
func (e *Expect) Number(value float64) *Number {
	opChain := e.chain.enter("Number()")
	defer opChain.leave()

	return newNumber(opChain, value)
}

// Deprecated: use NewBoolean or NewBooleanC instead.
func (e *Expect) Boolean(value bool) *Boolean {
	opChain := e.chain.enter("Boolean()")
	defer opChain.leave()

	return newBoolean(opChain, value)
}
