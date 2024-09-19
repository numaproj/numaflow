package httpexpect

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"moul.io/http2curl/v2"
)

// Printer is used to print requests and responses.
// CompactPrinter, DebugPrinter, and CurlPrinter implement this interface.
type Printer interface {
	// Request is called before request is sent.
	// It is allowed to read and close request body, or ignore it.
	Request(*http.Request)

	// Response is called after response is received.
	// It is allowed to read and close response body, or ignore it.
	Response(*http.Response, time.Duration)
}

// WebsocketPrinter is used to print writes and reads of WebSocket connection.
//
// If WebSocket connection is used, all Printers that also implement WebsocketPrinter
// are invoked on every WebSocket message read or written.
//
// DebugPrinter implements this interface.
type WebsocketPrinter interface {
	Printer

	// WebsocketWrite is called before writes to WebSocket connection.
	WebsocketWrite(typ int, content []byte, closeCode int)

	// WebsocketRead is called after reads from WebSocket connection.
	WebsocketRead(typ int, content []byte, closeCode int)
}

// CompactPrinter implements Printer.
// Prints requests in compact form. Does not print responses.
type CompactPrinter struct {
	logger Logger
}

// NewCompactPrinter returns a new CompactPrinter given a logger.
func NewCompactPrinter(logger Logger) CompactPrinter {
	return CompactPrinter{logger}
}

// Request implements Printer.Request.
func (p CompactPrinter) Request(req *http.Request) {
	if req != nil {
		p.logger.Logf("%s %s", req.Method, req.URL)
	}
}

// Response implements Printer.Response.
func (CompactPrinter) Response(*http.Response, time.Duration) {
}

// CurlPrinter implements Printer.
// Uses http2curl to dump requests as curl commands that can be inserted
// into terminal.
type CurlPrinter struct {
	logger Logger
}

// NewCurlPrinter returns a new CurlPrinter given a logger.
func NewCurlPrinter(logger Logger) CurlPrinter {
	return CurlPrinter{logger}
}

// Request implements Printer.Request.
func (p CurlPrinter) Request(req *http.Request) {
	if req != nil {
		cmd, err := http2curl.GetCurlCommand(req)
		if err != nil {
			panic(err)
		}
		p.logger.Logf("%s", cmd.String())
	}
}

// Response implements Printer.Response.
func (CurlPrinter) Response(*http.Response, time.Duration) {
}

// DebugPrinter implements Printer and WebsocketPrinter.
// Uses net/http/httputil to dump both requests and responses.
// Also prints all websocket messages.
type DebugPrinter struct {
	logger Logger
	body   bool
}

// NewDebugPrinter returns a new DebugPrinter given a logger and body
// flag. If body is true, request and response body is also printed.
func NewDebugPrinter(logger Logger, body bool) DebugPrinter {
	return DebugPrinter{logger, body}
}

// Request implements Printer.Request.
func (p DebugPrinter) Request(req *http.Request) {
	if req == nil {
		return
	}

	dump, err := httputil.DumpRequest(req, p.body)
	if err != nil {
		panic(err)
	}
	p.logger.Logf("%s", dump)
}

// Response implements Printer.Response.
func (p DebugPrinter) Response(resp *http.Response, duration time.Duration) {
	if resp == nil {
		return
	}

	dump, err := httputil.DumpResponse(resp, p.body)
	if err != nil {
		panic(err)
	}

	text := strings.Replace(string(dump), "\r\n", "\n", -1)
	lines := strings.SplitN(text, "\n", 2)

	p.logger.Logf("%s %s\n%s", lines[0], duration, lines[1])
}

// WebsocketWrite implements WebsocketPrinter.WebsocketWrite.
func (p DebugPrinter) WebsocketWrite(typ int, content []byte, closeCode int) {
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "-> Sent: %s", wsMessageType(typ))
	if typ == websocket.CloseMessage {
		fmt.Fprintf(b, " %s", wsCloseCode(closeCode))
	}
	fmt.Fprint(b, "\n")
	if len(content) > 0 {
		if typ == websocket.BinaryMessage {
			fmt.Fprintf(b, "%v\n", content)
		} else {
			fmt.Fprintf(b, "%s\n", content)
		}
	}
	fmt.Fprintf(b, "\n")
	p.logger.Logf(b.String())
}

// WebsocketRead implements WebsocketPrinter.WebsocketRead.
func (p DebugPrinter) WebsocketRead(typ int, content []byte, closeCode int) {
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "<- Received: %s", wsMessageType(typ))
	if typ == websocket.CloseMessage {
		fmt.Fprintf(b, " %s", wsCloseCode(closeCode))
	}
	fmt.Fprint(b, "\n")
	if len(content) > 0 {
		if typ == websocket.BinaryMessage {
			fmt.Fprintf(b, "%v\n", content)
		} else {
			fmt.Fprintf(b, "%s\n", content)
		}
	}
	fmt.Fprintf(b, "\n")
	p.logger.Logf(b.String())
}
