![](_images/logo.png)

# httpexpect [![GoDev](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/gavv/httpexpect/v2) [![Build](https://github.com/gavv/httpexpect/workflows/build/badge.svg)](https://github.com/gavv/httpexpect/actions) [![Coveralls](https://coveralls.io/repos/github/gavv/httpexpect/badge.svg?branch=master)](https://coveralls.io/github/gavv/httpexpect?branch=master) [![GitHub release](https://img.shields.io/github/tag/gavv/httpexpect.svg)](https://github.com/gavv/httpexpect/releases) [![Discord](https://img.shields.io/discord/1047473005900615780?logo=discord&label=discord&color=blueviolet&logoColor=white)](https://discord.gg/5SCPCuCWA9)

Concise, declarative, and easy to use end-to-end HTTP and REST API testing for Go (golang).

Basically, httpexpect is a set of chainable *builders* for HTTP requests and *assertions* for HTTP responses and payload, on top of net/http and several utility packages.

Workflow:

* Incrementally build HTTP requests.
* Inspect HTTP responses.
* Inspect response payload recursively.

## Features

##### Request builder

* URL path construction, with simple string interpolation provided by [`go-interpol`](https://github.com/imkira/go-interpol) package.
* URL query parameters (encoding using [`go-querystring`](https://github.com/google/go-querystring) package).
* Headers, cookies, payload: JSON,  urlencoded or multipart forms (encoding using [`form`](https://github.com/ajg/form) package), plain text.
* Custom reusable [request builders](#reusable-builders) and [request transformers](#request-transformers).

##### Response assertions

* Response status, predefined status ranges.
* Headers, cookies, payload: JSON, JSONP, forms, text.
* Round-trip time.
* Custom reusable [response matchers](#reusable-matchers).

##### Payload assertions

* Type-specific assertions, supported types: object, array, string, number, boolean, null, datetime.
* Regular expressions.
* Simple JSON queries (using subset of [JSONPath](http://goessner.net/articles/JsonPath/)), provided by [`jsonpath`](https://github.com/yalp/jsonpath) package.
* [JSON Schema](http://json-schema.org/) validation, provided by [`gojsonschema`](https://github.com/xeipuuv/gojsonschema) package.

##### WebSocket support (thanks to [@tyranron](https://github.com/tyranron))

* Upgrade an HTTP connection to a WebSocket connection (we use [`gorilla/websocket`](https://github.com/gorilla/websocket) internally).
* Interact with the WebSocket server.
* Inspect WebSocket connection parameters and WebSocket messages.

##### Pretty printing

* Verbose error messages.
* JSON diff is produced on failure using [`gojsondiff`](https://github.com/yudai/gojsondiff/) package.
* Failures are reported using [`testify`](https://github.com/stretchr/testify/) (`assert` or `require` package) or standard `testing` package.
* JSON values are pretty-printed using `encoding/json`, Go values are pretty-printed using [`litter`](https://github.com/sanity-io/litter).
* Dumping requests and responses in various formats, using [`httputil`](https://golang.org/pkg/net/http/httputil/), [`http2curl`](https://github.com/moul/http2curl), or simple compact logger.
* Color support using [`fatih/color`](https://github.com/fatih/color).

##### Tuning

* Tests can communicate with server via real HTTP client or invoke `net/http` or [`fasthttp`](https://github.com/valyala/fasthttp/) handler directly.
* User can provide custom HTTP client, WebSocket dialer, HTTP request factory (e.g. from the Google App Engine testing).
* User can configure formatting options or provide custom templates based on `text/template` engine.
* Custom handlers may be provided for logging, printing requests and responses, handling succeeded and failed assertions.

## Versions

The versions are selected according to the [semantic versioning](https://semver.org/) scheme. Every new major version gets its own stable branch with a backwards compatibility promise. Releases are tagged from stable branches.

The current stable branch is `v2`. Previous branches are still maintained, but no new features are added.

If you're using go.mod, use a versioned import path:

```go
import "github.com/gavv/httpexpect/v2"
```

Otherwise, use gopkg.in import path:

```go
import "gopkg.in/gavv/httpexpect.v2"
```

## Documentation

Documentation is available on [pkg.go.dev](https://pkg.go.dev/github.com/gavv/httpexpect/v2#section-documentation). It contains an overview and reference.

## Community

Community forum and Q&A board is right on GitHub in [discussions tab](https://github.com/gavv/httpexpect/discussions).

For more interactive discussion, you can join [discord chat](https://discord.gg/5SCPCuCWA9).

## Contributing

Feel free to report bugs, suggest improvements, and send pull requests! Please add documentation and tests for new features.

This project highly depends on contributors. Thank you all for your amazing work!

If you would like to submit code, see [HACKING.md](HACKING.md).

## Donating

If you would like to support my open-source work, you can do it here:

* [Liberapay](https://liberapay.com/gavv)
* [PayPal](https://www.paypal.com/paypalme/victorgaydov)

Thanks!

## Examples

See [`_examples`](_examples) directory for complete standalone examples.

* [`fruits_test.go`](_examples/fruits_test.go)

    Testing a simple CRUD server made with bare `net/http`.

* [`iris_test.go`](_examples/iris_test.go)

    Testing a server made with [`iris`](https://github.com/kataras/iris/) framework. Example includes JSON queries and validation, URL and form parameters, basic auth, sessions, and streaming. Tests invoke the `http.Handler` directly.

* [`echo_test.go`](_examples/echo_test.go)

    Testing a server with JWT authentication made with [`echo`](https://github.com/labstack/echo/) framework. Tests use either HTTP client or invoke the `http.Handler` directly.

* [`gin_test.go`](_examples/gin_test.go)

    Testing a server utilizing the [`gin`](https://github.com/gin-gonic/gin) web framework. Tests invoke the `http.Handler` directly.

* [`fasthttp_test.go`](_examples/fasthttp_test.go)

    Testing a server made with [`fasthttp`](https://github.com/valyala/fasthttp) package. Tests invoke the `fasthttp.RequestHandler` directly.

* [`websocket_test.go`](_examples/websocket_test.go)

    Testing a WebSocket server based on [`gorilla/websocket`](https://github.com/gorilla/websocket). Tests invoke the `http.Handler` or `fasthttp.RequestHandler` directly.

* [`oauth2_test.go`](_examples/oauth2_test.go)

  Testing a OAuth2 server with [`oauth2`](https://github.com/go-oauth2/oauth2/).

* [`gae_test.go`](_examples/gae_test.go)

    Testing a server running under the [Google App Engine](https://en.wikipedia.org/wiki/Google_App_Engine).

* [`formatter_test.go`](_examples/formatter_test.go)

    Testing with custom formatter for assertion messages.

## Quick start

##### Hello, world!

```go
package example

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gavv/httpexpect/v2"
)

func TestFruits(t *testing.T) {
	// create http.Handler
	handler := FruitsHandler()

	// run server using httptest
	server := httptest.NewServer(handler)
	defer server.Close()

	// create httpexpect instance
	e := httpexpect.Default(t, server.URL)

	// is it working?
	e.GET("/fruits").
		Expect().
		Status(http.StatusOK).JSON().Array().IsEmpty()
}
```

##### JSON

```go
orange := map[string]interface{}{
	"weight": 100,
}

e.PUT("/fruits/orange").WithJSON(orange).
	Expect().
	Status(http.StatusNoContent).NoContent()

e.GET("/fruits/orange").
	Expect().
	Status(http.StatusOK).
	JSON().Object().ContainsKey("weight").HasValue("weight", 100)

apple := map[string]interface{}{
	"colors": []interface{}{"green", "red"},
	"weight": 200,
}

e.PUT("/fruits/apple").WithJSON(apple).
	Expect().
	Status(http.StatusNoContent).NoContent()

obj := e.GET("/fruits/apple").
	Expect().
	Status(http.StatusOK).JSON().Object()

obj.Keys().ContainsOnly("colors", "weight")

obj.Value("colors").Array().ConsistsOf("green", "red")
obj.Value("colors").Array().Value(0).String().IsEqual("green")
obj.Value("colors").Array().Value(1).String().IsEqual("red")
obj.Value("colors").Array().First().String().IsEqual("green")
obj.Value("colors").Array().Last().String().IsEqual("red")
```

##### JSON Schema and JSON Path

```go
schema := `{
	"type": "array",
	"items": {
		"type": "object",
		"properties": {
			...
			"private": {
				"type": "boolean"
			}
		}
	}
}`

repos := e.GET("/repos/octocat").
	Expect().
	Status(http.StatusOK).JSON()

// validate JSON schema
repos.Schema(schema)

// run JSONPath query and iterate results
for _, private := range repos.Path("$..private").Array().Iter() {
	private.Boolean().IsFalse()
}
```

##### JSON decoding

```go
type User struct {
	Name   string `json:"name"`
	Age    int    `json:"age"`
	Gender string `json:"gender"`
}

var user User
e.GET("/user").
	Expect().
	Status(http.StatusOK).
	JSON().
	Decode(&user)
	
if user.Name != "octocat" {
	t.Fail()
}
```

##### Forms

```go
// post form encoded from struct or map
e.POST("/form").WithForm(structOrMap).
	Expect().
	Status(http.StatusOK)

// set individual fields
e.POST("/form").WithFormField("foo", "hello").WithFormField("bar", 123).
	Expect().
	Status(http.StatusOK)

// multipart form
e.POST("/form").WithMultipart().
	WithFile("avatar", "./john.png").WithFormField("username", "john").
	Expect().
	Status(http.StatusOK)
```

##### URL construction

```go
// construct path using ordered parameters
e.GET("/repos/{user}/{repo}", "octocat", "hello-world").
	Expect().
	Status(http.StatusOK)

// construct path using named parameters
e.GET("/repos/{user}/{repo}").
	WithPath("user", "octocat").WithPath("repo", "hello-world").
	Expect().
	Status(http.StatusOK)

// set query parameters
e.GET("/repos/{user}", "octocat").WithQuery("sort", "asc").
	Expect().
	Status(http.StatusOK)    // "/repos/octocat?sort=asc"
```

##### Headers

```go
// set If-Match
e.POST("/users/john").WithHeader("If-Match", etag).WithJSON(john).
	Expect().
	Status(http.StatusOK)

// check ETag
e.GET("/users/john").
	Expect().
	Status(http.StatusOK).Header("ETag").NotEmpty()

// check Date
t := time.Now()

e.GET("/users/john").
	Expect().
	Status(http.StatusOK).Header("Date").AsDateTime().InRange(t, time.Now())
```

##### Cookies

```go
// set cookie
t := time.Now()

e.POST("/users/john").WithCookie("session", sessionID).WithJSON(john).
	Expect().
	Status(http.StatusOK)

// check cookies
c := e.GET("/users/john").
	Expect().
	Status(http.StatusOK).Cookie("session")

c.Value().IsEqual(sessionID)
c.Domain().IsEqual("example.com")
c.Path().IsEqual("/")
c.Expires().InRange(t, t.Add(time.Hour * 24))
```

##### Regular expressions

```go
// simple match
e.GET("/users/john").
	Expect().
	Header("Location").
	Match("http://(.+)/users/(.+)").Values("example.com", "john")

// check capture groups by index or name
m := e.GET("/users/john").
	Expect().
	Header("Location").Match("http://(?P<host>.+)/users/(?P<user>.+)")

m.Index(0).IsEqual("http://example.com/users/john")
m.Index(1).IsEqual("example.com")
m.Index(2).IsEqual("john")

m.Name("host").IsEqual("example.com")
m.Name("user").IsEqual("john")
```

##### Redirection support

```go
e.POST("/path").
	WithRedirectPolicy(httpexpect.FollowAllRedirects).
	WithMaxRedirects(5).
	Expect().
	Status(http.StatusOK)

e.POST("/path").
	WithRedirectPolicy(httpexpect.DontFollowRedirects).
	Expect().
	Status(http.StatusPermanentRedirect)
```

##### Retry support

```go
// default retry policy
e.POST("/path").
	WithMaxRetries(5).
	Expect().
	Status(http.StatusOK)

// custom retry policy
e.POST("/path").
	WithMaxRetries(5).
	WithRetryPolicy(httpexpect.RetryAllErrors).
	Expect().
	Status(http.StatusOK)

// custom retry delays
e.POST("/path").
	WithMaxRetries(5).
	WithRetryDelay(time.Second, time.Minute).
	Expect().
	Status(http.StatusOK)
```

##### Subdomains and per-request URL

```go
e.GET("/path").WithURL("http://example.com").
	Expect().
	Status(http.StatusOK)

e.GET("/path").WithURL("http://subdomain.example.com").
	Expect().
	Status(http.StatusOK)
```

##### WebSocket support

```go
ws := e.GET("/mysocket").WithWebsocketUpgrade().
	Expect().
	Status(http.StatusSwitchingProtocols).
	Websocket()
defer ws.Disconnect()

ws.WriteText("some request").
	Expect().
	TextMessage().Body().IsEqual("some response")

ws.CloseWithText("bye").
	Expect().
	CloseMessage().NoContent()
```

##### Reusable builders

```go
e := httpexpect.Default(t, "http://example.com")

r := e.POST("/login").WithForm(Login{"ford", "betelgeuse7"}).
	Expect().
	Status(http.StatusOK).JSON().Object()

token := r.Value("token").String().Raw()

auth := e.Builder(func (req *httpexpect.Request) {
	req.WithHeader("Authorization", "Bearer "+token)
})

auth.GET("/restricted").
	Expect().
	Status(http.StatusOK)

e.GET("/restricted").
	Expect().
	Status(http.StatusUnauthorized)
```

##### Reusable matchers

```go
e := httpexpect.Default(t, "http://example.com")

// every response should have this header
m := e.Matcher(func (resp *httpexpect.Response) {
	resp.Header("API-Version").NotEmpty()
})

m.GET("/some-path").
	Expect().
	Status(http.StatusOK)

m.GET("/bad-path").
	Expect().
	Status(http.StatusNotFound)
```

##### Request transformers

```go
e := httpexpect.Default(t, "http://example.com")

myTranform := func(r* http.Request) {
	// modify the underlying http.Request
}

// apply transformer to a single request
e.POST("/some-path").
	WithTransformer(myTranform).
	Expect().
	Status(http.StatusOK)

// create a builder that applies transfromer to every request
myBuilder := e.Builder(func (req *httpexpect.Request) {
	req.WithTransformer(myTranform)
})

myBuilder.POST("/some-path").
	Expect().
	Status(http.StatusOK)
```

##### Shared environment

```go
e := httpexpect.Default(t, "http://example.com")

t.Run("/users", func(t *testing.T) {
	obj := e.GET("/users").
		Expect().
		Status(http.StatusOK).JSON().Object()

	// store user id for next tests
	userID := obj.Path("$.users[1].id").String().Raw()
	e.Env().Put("user1.id", userID)
})

t.Run("/user/{userId}", func(t *testing.T) {
	// read user id from previous tests
	userID := e.Env().GetString("user1.id")

	e.GET("/user/{userId}").
		WithPath("userId", userID)
		Expect().
		Status(http.StatusOK)
})
```

##### Custom config

```go
e := httpexpect.WithConfig(httpexpect.Config{
	// include test name in failures (optional)
	TestName: t.Name(),

	// prepend this url to all requests
	BaseURL: "http://example.com",

	// use http.Client with a cookie jar and timeout
	Client: &http.Client{
		Jar:     httpexpect.NewCookieJar(),
		Timeout: time.Second * 30,
	},

	// use fatal failures
	Reporter: httpexpect.NewRequireReporter(t),

	// print all requests and responses
	Printers: []httpexpect.Printer{
		httpexpect.NewDebugPrinter(t, true),
	},
})
```

##### Use HTTP handler directly

```go
// invoke http.Handler directly using httpexpect.Binder
var handler http.Handler = myHandler()

e := httpexpect.WithConfig(httpexpect.Config{
	// prepend this url to all requests, required for cookies
	// to be handled correctly
	BaseURL: "http://example.com",
	Reporter: httpexpect.NewAssertReporter(t),
	Client: &http.Client{
		Transport: httpexpect.NewBinder(handler),
		Jar:       httpexpect.NewCookieJar(),
	},
})

// invoke fasthttp.RequestHandler directly using httpexpect.FastBinder
var handler fasthttp.RequestHandler = myHandler()

e := httpexpect.WithConfig(httpexpect.Config{
	// prepend this url to all requests, required for cookies
	// to be handled correctly
	BaseURL: "http://example.com",
	Reporter: httpexpect.NewAssertReporter(t),
	Client: &http.Client{
		Transport: httpexpect.NewFastBinder(handler),
		Jar:       httpexpect.NewCookieJar(),
	},
})
```

##### Per-request client or handler

```go
e := httpexpect.Default(t, server.URL)

client := &http.Client{
	Transport: &http.Transport{
		DisableCompression: true,
	},
}

// overwrite client
e.GET("/path").WithClient(client).
	Expect().
	Status(http.StatusOK)

// construct client that invokes a handler directly and overwrite client
e.GET("/path").WithHandler(handler).
	Expect().
	Status(http.StatusOK)
```

##### WebSocket dialer

```go
// invoke http.Handler directly using websocket.Dialer
var handler http.Handler = myHandler()

e := httpexpect.WithConfig(httpexpect.Config{
	BaseURL:         "http://example.com",
	Reporter:        httpexpect.NewAssertReporter(t),
	WebsocketDialer: httpexpect.NewWebsocketDialer(handler),
})

// invoke fasthttp.RequestHandler directly using websocket.Dialer
var handler fasthttp.RequestHandler = myHandler()

e := httpexpect.WithConfig(httpexpect.Config{
	BaseURL:         "http://example.com",
	Reporter:        httpexpect.NewAssertReporter(t),
	WebsocketDialer: httpexpect.NewFastWebsocketDialer(handler),
})
```

##### Session support

```go
// cookie jar is used to store cookies from server
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter: httpexpect.NewAssertReporter(t),
	Client: &http.Client{
		Jar: httpexpect.NewCookieJar(), // used by default if Client is nil
	},
})

// cookies are disabled
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter: httpexpect.NewAssertReporter(t),
	Client: &http.Client{
		Jar: nil,
	},
})
```

##### TLS support

```go
// use TLS with http.Transport
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter: httpexpect.NewAssertReporter(t),
	Client: &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				// accept any certificate; for testing only!
				InsecureSkipVerify: true,
			},
		},
	},
})

// use TLS with http.Handler
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter: httpexpect.NewAssertReporter(t),
	Client: &http.Client{
		Transport: &httpexpect.Binder{
			Handler: myHandler,
			TLS:     &tls.ConnectionState{},
		},
	},
})
```

##### Proxy support

```go
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter: httpexpect.NewAssertReporter(t),
	Client: &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL("http://proxy.example.com"),
		},
	},
})
```

##### Global time-out/cancellation

```go
handler := FruitsHandler()

server := httptest.NewServer(handler)
defer server.Close()

ctx, cancel := context.WithCancel(context.Background())

e := WithConfig(Config{
	BaseURL:  server.URL,
	Reporter: httpexpect.NewAssertReporter(t),
	Context:  ctx,
})

go func() {
	time.Sleep(time.Duration(5)*time.Second)
	cancel()
}()

e.GET("/fruits").
	Expect().
	Status(http.StatusOK)
```

##### Per-request time-out/cancellation

```go
// per-request context
e.GET("/fruits").
	WithContext(context.TODO()).
	Expect().
	Status(http.StatusOK)

// per-request timeout
e.GET("/fruits").
	WithTimeout(time.Duration(5)*time.Second).
	Expect().
	Status(http.StatusOK)

// timeout combined with retries (timeout applies to each try)
e.POST("/fruits").
	WithMaxRetries(5).
	WithTimeout(time.Duration(10)*time.Second).
	Expect().
	Status(http.StatusOK)
```

##### Support for aliases in failure messages

```go
// when the tests fails, assertion path in the failure message is:
//   Request("GET").Expect().JSON().Array().IsEmpty()
e.GET("/fruits").
	Expect().
	Status(http.StatusOK).JSON().Array().IsEmpty()


// assign alias "fruits" to the Array variable
fruits := e.GET("/fruits").
	Expect().
	Status(http.StatusOK).JSON().Array().Alias("fruits")

// assertion path in the failure message is now:
//   fruits.IsEmpty()
fruits.IsEmpty()
```

##### Printing requests and responses

```go
// print requests in short form, don't print responses
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter: httpexpect.NewAssertReporter(t),
	Printers: []httpexpect.Printer{
		httpexpect.NewCompactPrinter(t),
	},
})

// print requests as curl commands that can be inserted into terminal
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter: httpexpect.NewAssertReporter(t),
	Printers: []httpexpect.Printer{
		httpexpect.NewCurlPrinter(t),
	},
})

// print requests and responses in verbose form
// also print all incoming and outgoing websocket messages
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter: httpexpect.NewAssertReporter(t),
	Printers: []httpexpect.Printer{
		httpexpect.NewDebugPrinter(t, true),
	},
})
```

##### Customize failure formatting

```go
// customize formatting options
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter:  httpexpect.NewAssertReporter(t),
	Formatter: &httpexpect.DefaultFormatter{
		DisablePaths: true,
		DisableDiffs: true,
		FloatFormat:  httpexpect.FloatFormatScientific,
		ColorMode:    httpexpect.ColorModeNever,
		LineWidth:    80,
	},
})

// customize formatting template
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter:  httpexpect.NewAssertReporter(t),
	Formatter: &httpexpect.DefaultFormatter{
		SuccessTemplate: "...",
		FailureTemplate: "...",
		TemplateFuncs:   template.FuncMap{ ... },
	},
})

// provide custom formatter
e := httpexpect.WithConfig(httpexpect.Config{
	Reporter:  httpexpect.NewAssertReporter(t),
	Formatter: &MyFormatter{},
})
```

##### Customize assertion handling

```go
// enable printing of succeeded assertions
e := httpexpect.WithConfig(httpexpect.Config{
	AssertionHandler: &httpexpect.DefaultAssertionHandler{
		Formatter: &httpexpect.DefaultFormatter{},
		Reporter:  httpexpect.NewAssertReporter(t),
		Logger:    t, // specify logger to enable printing of succeeded assertions
	},
})

// provide custom assertion handler
// here you can implement custom handling of succeeded and failed assertions
// this may be useful for integrating httpexpect with other testing libs
// if desired, you can completely ignore builtin Formatter, Reporter, and Logger
e := httpexpect.WithConfig(httpexpect.Config{
	AssertionHandler: &MyAssertionHandler{},
})
```

## Similar packages

* [`gorequest`](https://github.com/parnurzeal/gorequest)
* [`baloo`](https://github.com/h2non/baloo)
* [`apitest`](https://github.com/steinfletcher/apitest)
* [`gofight`](https://github.com/appleboy/gofight)
* [`frisby`](https://github.com/verdverm/frisby)
* [`forest`](https://github.com/emicklei/forest)
* [`restit`](https://github.com/go-restit/restit)
* [`httptesting`](https://github.com/dolab/httptesting)
* [`http-test`](https://github.com/vsco/http-test)
* [`go-json-rest`](https://github.com/ant0ine/go-json-rest)

## License

[MIT](LICENSE)
