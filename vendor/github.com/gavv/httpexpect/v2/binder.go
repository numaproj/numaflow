package httpexpect

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"

	"github.com/valyala/fasthttp"
)

// Binder implements networkless http.RoundTripper attached directly to
// http.Handler.
//
// Binder emulates network communication by invoking given http.Handler
// directly. It passes httptest.ResponseRecorder as http.ResponseWriter
// to the handler, and then constructs http.Response from recorded data.
type Binder struct {
	// HTTP handler invoked for every request.
	Handler http.Handler
	// TLS connection state used for https:// requests.
	TLS *tls.ConnectionState
}

// NewBinder returns a new Binder given a http.Handler.
//
// Example:
//
//	client := &http.Client{
//		Transport: NewBinder(handler),
//	}
func NewBinder(handler http.Handler) Binder {
	return Binder{Handler: handler}
}

// RoundTrip implements http.RoundTripper.RoundTrip.
func (binder Binder) RoundTrip(origReq *http.Request) (*http.Response, error) {
	req := *origReq

	if req.Proto == "" {
		req.Proto = fmt.Sprintf("HTTP/%d.%d", req.ProtoMajor, req.ProtoMinor)
	}

	if req.Body != nil && req.Body != http.NoBody {
		if req.ContentLength == -1 {
			req.TransferEncoding = []string{"chunked"}
		}
	} else {
		req.Body = http.NoBody
	}

	if req.URL != nil && req.URL.Scheme == "https" && binder.TLS != nil {
		req.TLS = binder.TLS
	}

	if req.RequestURI == "" {
		req.RequestURI = req.URL.RequestURI()
	}

	recorder := httptest.NewRecorder()

	binder.Handler.ServeHTTP(recorder, &req)

	resp := http.Response{
		Request:    &req,
		StatusCode: recorder.Code,
		Status:     http.StatusText(recorder.Code),
		Header:     recorder.Result().Header,
	}

	if recorder.Flushed {
		resp.TransferEncoding = []string{"chunked"}
	}

	if recorder.Body != nil {
		resp.Body = io.NopCloser(recorder.Body)
	}

	return &resp, nil
}

// FastBinder implements networkless http.RoundTripper attached directly
// to fasthttp.RequestHandler.
//
// FastBinder emulates network communication by invoking given fasthttp.RequestHandler
// directly. It converts http.Request to fasthttp.Request, invokes handler, and then
// converts fasthttp.Response to http.Response.
type FastBinder struct {
	// FastHTTP handler invoked for every request.
	Handler fasthttp.RequestHandler
	// TLS connection state used for https:// requests.
	TLS *tls.ConnectionState
	// If non-nil, fasthttp.RequestCtx.Logger() will print messages to it.
	Logger Logger
}

// NewFastBinder returns a new FastBinder given a fasthttp.RequestHandler.
//
// Example:
//
//	client := &http.Client{
//		Transport: NewFastBinder(fasthandler),
//	}
func NewFastBinder(handler fasthttp.RequestHandler) FastBinder {
	return FastBinder{Handler: handler}
}

// RoundTrip implements http.RoundTripper.RoundTrip.
func (binder FastBinder) RoundTrip(stdreq *http.Request) (*http.Response, error) {
	fastreq := std2fast(stdreq)

	var conn net.Conn
	if stdreq.URL != nil && stdreq.URL.Scheme == "https" && binder.TLS != nil {
		conn = connTLS{state: binder.TLS}
	} else {
		conn = connNonTLS{}
	}

	ctx := fasthttp.RequestCtx{}
	log := fastLogger{binder.Logger}
	ctx.Init2(conn, log, true)

	fastreq.CopyTo(&ctx.Request)

	if stdreq.RemoteAddr != "" {
		var parts = strings.SplitN(stdreq.RemoteAddr, ":", 2)
		host := parts[0]
		port := 0
		if len(parts) > 1 {
			port, _ = strconv.Atoi(parts[1])
		}
		ctx.SetRemoteAddr(&net.TCPAddr{
			IP:   net.ParseIP(host),
			Port: port,
		})
	}

	if stdreq.ContentLength >= 0 {
		ctx.Request.Header.SetContentLength(int(stdreq.ContentLength))
	} else {
		ctx.Request.Header.SetContentLength(-1)
		ctx.Request.Header.Add("Transfer-Encoding", "chunked")
	}

	if stdreq.Body != nil {
		b, err := io.ReadAll(stdreq.Body)
		if err == nil {
			ctx.Request.SetBody(b)
		}
	}

	binder.Handler(&ctx)

	return fast2std(stdreq, &ctx.Response), nil
}

func std2fast(stdreq *http.Request) *fasthttp.Request {
	fastreq := &fasthttp.Request{}

	fastreq.SetRequestURI(stdreq.URL.String())

	if stdreq.Proto != "" {
		fastreq.Header.SetProtocol(stdreq.Proto)
	} else if stdreq.ProtoMajor != 0 || stdreq.ProtoMinor != 0 {
		fastreq.Header.SetProtocol(
			fmt.Sprintf("HTTP/%d.%d", stdreq.ProtoMajor, stdreq.ProtoMinor))
	}

	fastreq.Header.SetMethod(stdreq.Method)

	if stdreq.Host != "" {
		fastreq.Header.SetHost(stdreq.Host)
	}

	for k, a := range stdreq.Header {
		for n, v := range a {
			if n == 0 {
				fastreq.Header.Set(k, v)
			} else {
				fastreq.Header.Add(k, v)
			}
		}
	}

	fastreq.Header.SetContentLength(int(stdreq.ContentLength))

	return fastreq
}

func fast2std(stdreq *http.Request, fastresp *fasthttp.Response) *http.Response {
	status := fastresp.Header.StatusCode()
	body := fastresp.Body()

	stdresp := &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Request:    stdreq,
	}

	fastresp.Header.VisitAll(func(k, v []byte) {
		sk := string(k)
		sv := string(v)
		if stdresp.Header == nil {
			stdresp.Header = make(http.Header)
		}
		stdresp.Header.Add(sk, sv)
	})

	if fastresp.Header.ContentLength() >= 0 {
		stdresp.ContentLength = int64(fastresp.Header.ContentLength())
	} else {
		stdresp.ContentLength = -1
		stdresp.TransferEncoding = []string{"chunked"}
	}

	if body != nil {
		stdresp.Body = io.NopCloser(bytes.NewReader(body))
	} else {
		stdresp.Body = io.NopCloser(bytes.NewReader(nil))
	}

	return stdresp
}

type fastLogger struct {
	logger Logger
}

func (f fastLogger) Printf(format string, args ...interface{}) {
	if f.logger != nil {
		f.logger.Logf(format, args...)
	}
}

type connNonTLS struct {
	net.Conn
}

func (connNonTLS) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero}
}

func (connNonTLS) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero}
}

type connTLS struct {
	connNonTLS
	state *tls.ConnectionState
}

func (c connTLS) Handshake() error {
	return nil
}

func (c connTLS) ConnectionState() tls.ConnectionState {
	return *c.state
}
