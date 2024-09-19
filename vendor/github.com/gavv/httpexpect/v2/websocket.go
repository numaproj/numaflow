package httpexpect

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

var (
	noDuration   = time.Duration(0)
	infiniteTime = time.Time{}
)

// WebsocketConn is used by Websocket to communicate with actual WebSocket connection.
type WebsocketConn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	Close() error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	Subprotocol() string
}

// Websocket provides methods to read from, write into and close WebSocket
// connection.
type Websocket struct {
	noCopy noCopy
	config Config
	chain  *chain

	conn WebsocketConn

	readTimeout  time.Duration
	writeTimeout time.Duration

	isClosed bool
}

// Deprecated: use NewWebsocketC instead.
func NewWebsocket(config Config, conn WebsocketConn) *Websocket {
	return NewWebsocketC(config, conn)
}

// NewWebsocketC returns a new Websocket instance.
//
// Requirements for config are same as for WithConfig function.
func NewWebsocketC(config Config, conn WebsocketConn) *Websocket {
	config = config.withDefaults()

	return newWebsocket(
		newChainWithConfig("Websocket()", config),
		config,
		conn,
	)
}

func newWebsocket(parent *chain, config Config, conn WebsocketConn) *Websocket {
	config.validate()

	return &Websocket{
		config: config,
		chain:  parent.clone(),
		conn:   conn,
	}
}

// Conn returns underlying WebsocketConn object.
// This is the value originally passed to NewConnection.
func (ws *Websocket) Conn() WebsocketConn {
	return ws.conn
}

// Deprecated: use Conn instead.
func (ws *Websocket) Raw() *websocket.Conn {
	if ws.conn == nil {
		return nil
	}
	conn, ok := ws.conn.(*websocket.Conn)
	if !ok {
		return nil
	}
	return conn
}

// Alias is similar to Value.Alias.
func (ws *Websocket) Alias(name string) *Websocket {
	opChain := ws.chain.enter("Alias(%q)", name)
	defer opChain.leave()

	ws.chain.setAlias(name)
	return ws
}

// WithReadTimeout sets timeout duration for WebSocket connection reads.
//
// By default no timeout is used.
func (ws *Websocket) WithReadTimeout(timeout time.Duration) *Websocket {
	opChain := ws.chain.enter("WithReadTimeout()")
	defer opChain.leave()

	if opChain.failed() {
		return ws
	}

	ws.readTimeout = timeout

	return ws
}

// WithoutReadTimeout removes timeout for WebSocket connection reads.
func (ws *Websocket) WithoutReadTimeout() *Websocket {
	opChain := ws.chain.enter("WithoutReadTimeout()")
	defer opChain.leave()

	if opChain.failed() {
		return ws
	}

	ws.readTimeout = noDuration

	return ws
}

// WithWriteTimeout sets timeout duration for WebSocket connection writes.
//
// By default no timeout is used.
func (ws *Websocket) WithWriteTimeout(timeout time.Duration) *Websocket {
	opChain := ws.chain.enter("WithWriteTimeout()")
	defer opChain.leave()

	if opChain.failed() {
		return ws
	}

	ws.writeTimeout = timeout

	return ws
}

// WithoutWriteTimeout removes timeout for WebSocket connection writes.
//
// If not used then DefaultWebsocketTimeout will be used.
func (ws *Websocket) WithoutWriteTimeout() *Websocket {
	opChain := ws.chain.enter("WithoutWriteTimeout()")
	defer opChain.leave()

	if opChain.failed() {
		return ws
	}

	ws.writeTimeout = noDuration

	return ws
}

// Subprotocol returns a new String instance with negotiated protocol
// for the connection.
func (ws *Websocket) Subprotocol() *String {
	opChain := ws.chain.enter("Subprotocol()")
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	if ws.conn == nil {
		return newString(opChain, "")
	}

	return newString(opChain, ws.conn.Subprotocol())
}

// Expect reads next message from WebSocket connection and
// returns a new WebsocketMessage instance.
//
// Example:
//
//	msg := conn.Expect()
//	msg.JSON().Object().HasValue("message", "hi")
func (ws *Websocket) Expect() *WebsocketMessage {
	opChain := ws.chain.enter("Expect()")
	defer opChain.leave()

	if ws.checkUnusable(opChain, "Expect()") {
		return newEmptyWebsocketMessage(opChain)
	}

	m := ws.readMessage(opChain)
	if m == nil {
		return newEmptyWebsocketMessage(opChain)
	}

	return m
}

// Disconnect closes the underlying WebSocket connection without sending or
// waiting for a close message.
//
// It's okay to call this function multiple times.
//
// It's recommended to always call this function after connection usage is over
// to ensure that no resource leaks will happen.
//
// Example:
//
//	conn := resp.Connection()
//	defer conn.Disconnect()
func (ws *Websocket) Disconnect() *Websocket {
	opChain := ws.chain.enter("Disconnect()")
	defer opChain.leave()

	if ws.conn == nil || ws.isClosed {
		return ws
	}

	ws.isClosed = true

	if err := ws.conn.Close(); err != nil {
		opChain.fail(AssertionFailure{
			Type: AssertOperation,
			Errors: []error{
				errors.New("got close error when disconnecting websocket"),
				err,
			},
		})
	}

	return ws
}

// Close cleanly closes the underlying WebSocket connection
// by sending an empty close message and then waiting (with timeout)
// for the server to close the connection.
//
// WebSocket close code may be optionally specified.
// If not, then "1000 - Normal Closure" will be used.
//
// WebSocket close codes are defined in RFC 6455, section 11.7.
// See also https://godoc.org/github.com/gorilla/websocket#pkg-constants
//
// It's okay to call this function multiple times.
//
// Example:
//
//	conn := resp.Connection()
//	conn.Close(websocket.CloseUnsupportedData)
func (ws *Websocket) Close(code ...int) *Websocket {
	opChain := ws.chain.enter("Close()")
	defer opChain.leave()

	switch {
	case ws.checkUnusable(opChain, "Close()"):
		return ws

	case len(code) > 1:
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple code arguments"),
			},
		})
		return ws
	}

	ws.writeMessage(opChain, websocket.CloseMessage, nil, code...)

	return ws
}

// CloseWithBytes cleanly closes the underlying WebSocket connection
// by sending given slice of bytes as a close message and then waiting
// (with timeout) for the server to close the connection.
//
// WebSocket close code may be optionally specified.
// If not, then "1000 - Normal Closure" will be used.
//
// WebSocket close codes are defined in RFC 6455, section 11.7.
// See also https://godoc.org/github.com/gorilla/websocket#pkg-constants
//
// It's okay to call this function multiple times.
//
// Example:
//
//	conn := resp.Connection()
//	conn.CloseWithBytes([]byte("bye!"), websocket.CloseGoingAway)
func (ws *Websocket) CloseWithBytes(b []byte, code ...int) *Websocket {
	opChain := ws.chain.enter("CloseWithBytes()")
	defer opChain.leave()

	switch {
	case ws.checkUnusable(opChain, "CloseWithBytes()"):
		return ws

	case len(code) > 1:
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple code arguments"),
			},
		})
		return ws
	}

	ws.writeMessage(opChain, websocket.CloseMessage, b, code...)

	return ws
}

// CloseWithJSON cleanly closes the underlying WebSocket connection
// by sending given object (marshaled using json.Marshal()) as a close message
// and then waiting (with timeout) for the server to close the connection.
//
// WebSocket close code may be optionally specified.
// If not, then "1000 - Normal Closure" will be used.
//
// WebSocket close codes are defined in RFC 6455, section 11.7.
// See also https://godoc.org/github.com/gorilla/websocket#pkg-constants
//
// It's okay to call this function multiple times.
//
// Example:
//
//	type MyJSON struct {
//	  Foo int `json:"foo"`
//	}
//
//	conn := resp.Connection()
//	conn.CloseWithJSON(MyJSON{Foo: 123}, websocket.CloseUnsupportedData)
func (ws *Websocket) CloseWithJSON(
	object interface{}, code ...int,
) *Websocket {
	opChain := ws.chain.enter("CloseWithJSON()")
	defer opChain.leave()

	switch {
	case ws.checkUnusable(opChain, "CloseWithJSON()"):
		return ws

	case len(code) > 1:
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple code arguments"),
			},
		})
		return ws
	}

	b, err := json.Marshal(object)

	if err != nil {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{object},
			Errors: []error{
				errors.New("invalid json object"),
				err,
			},
		})
		return ws
	}

	ws.writeMessage(opChain, websocket.CloseMessage, b, code...)

	return ws
}

// CloseWithText cleanly closes the underlying WebSocket connection
// by sending given text as a close message and then waiting (with timeout)
// for the server to close the connection.
//
// WebSocket close code may be optionally specified.
// If not, then "1000 - Normal Closure" will be used.
//
// WebSocket close codes are defined in RFC 6455, section 11.7.
// See also https://godoc.org/github.com/gorilla/websocket#pkg-constants
//
// It's okay to call this function multiple times.
//
// Example:
//
//	conn := resp.Connection()
//	conn.CloseWithText("bye!")
func (ws *Websocket) CloseWithText(s string, code ...int) *Websocket {
	opChain := ws.chain.enter("CloseWithText()")
	defer opChain.leave()

	switch {
	case ws.checkUnusable(opChain, "CloseWithText()"):
		return ws

	case len(code) > 1:
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple code arguments"),
			},
		})
		return ws
	}

	ws.writeMessage(opChain, websocket.CloseMessage, []byte(s), code...)

	return ws
}

// WriteMessage writes to the underlying WebSocket connection a message
// of given type with given content.
// Additionally, WebSocket close code may be specified for close messages.
//
// WebSocket message types are defined in RFC 6455, section 11.8.
// See also https://godoc.org/github.com/gorilla/websocket#pkg-constants
//
// WebSocket close codes are defined in RFC 6455, section 11.7.
// See also https://godoc.org/github.com/gorilla/websocket#pkg-constants
//
// Example:
//
//	conn := resp.Connection()
//	conn.WriteMessage(websocket.CloseMessage, []byte("Namárië..."))
func (ws *Websocket) WriteMessage(typ int, content []byte, closeCode ...int) *Websocket {
	opChain := ws.chain.enter("WriteMessage()")
	defer opChain.leave()

	if ws.checkUnusable(opChain, "WriteMessage()") {
		return ws
	}

	ws.writeMessage(opChain, typ, content, closeCode...)

	return ws
}

// WriteBytesBinary is a shorthand for c.WriteMessage(websocket.BinaryMessage, b).
func (ws *Websocket) WriteBytesBinary(b []byte) *Websocket {
	opChain := ws.chain.enter("WriteBytesBinary()")
	defer opChain.leave()

	if ws.checkUnusable(opChain, "WriteBytesBinary()") {
		return ws
	}

	ws.writeMessage(opChain, websocket.BinaryMessage, b)

	return ws
}

// WriteBytesText is a shorthand for c.WriteMessage(websocket.TextMessage, b).
func (ws *Websocket) WriteBytesText(b []byte) *Websocket {
	opChain := ws.chain.enter("WriteBytesText()")
	defer opChain.leave()

	if ws.checkUnusable(opChain, "WriteBytesText()") {
		return ws
	}

	ws.writeMessage(opChain, websocket.TextMessage, b)

	return ws
}

// WriteText is a shorthand for
// c.WriteMessage(websocket.TextMessage, []byte(s)).
func (ws *Websocket) WriteText(s string) *Websocket {
	opChain := ws.chain.enter("WriteText()")
	defer opChain.leave()

	if ws.checkUnusable(opChain, "WriteText()") {
		return ws
	}

	return ws.WriteMessage(websocket.TextMessage, []byte(s))
}

// WriteJSON writes to the underlying WebSocket connection given object,
// marshaled using json.Marshal().
func (ws *Websocket) WriteJSON(object interface{}) *Websocket {
	opChain := ws.chain.enter("WriteJSON()")
	defer opChain.leave()

	if ws.checkUnusable(opChain, "WriteJSON()") {
		return ws
	}

	b, err := json.Marshal(object)

	if err != nil {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{object},
			Errors: []error{
				errors.New("invalid json object"),
				err,
			},
		})
		return ws
	}

	ws.writeMessage(opChain, websocket.TextMessage, b)

	return ws
}

func (ws *Websocket) checkUnusable(opChain *chain, where string) bool {
	switch {
	case opChain.failed():
		return true

	case ws.conn == nil:
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				fmt.Errorf("unexpected %s call for failed websocket connection", where),
			},
		})
		return true

	case ws.isClosed:
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				fmt.Errorf("unexpected %s call for closed websocket connection", where),
			},
		})
		return true
	}

	return false
}

func (ws *Websocket) readMessage(opChain *chain) *WebsocketMessage {
	wm := newEmptyWebsocketMessage(opChain)

	if !ws.setReadDeadline(opChain) {
		return nil
	}

	var err error
	wm.typ, wm.content, err = ws.conn.ReadMessage()

	if err != nil {
		closeErr, ok := err.(*websocket.CloseError)
		if !ok {
			opChain.fail(AssertionFailure{
				Type: AssertOperation,
				Errors: []error{
					errors.New("failed to read from websocket"),
					err,
				},
			})
			return nil
		}

		wm.typ = websocket.CloseMessage
		wm.closeCode = closeErr.Code
		wm.content = []byte(closeErr.Text)
	}

	ws.printRead(wm.typ, wm.content, wm.closeCode)

	return wm
}

func (ws *Websocket) writeMessage(
	opChain *chain, typ int, content []byte, closeCode ...int,
) {
	switch typ {
	case websocket.TextMessage, websocket.BinaryMessage:
		ws.printWrite(typ, content, 0)

	case websocket.CloseMessage:
		if len(closeCode) > 1 {
			opChain.fail(AssertionFailure{
				Type: AssertUsage,
				Errors: []error{
					errors.New("unexpected multiple closeCode arguments"),
				},
			})
			return
		}

		code := websocket.CloseNormalClosure
		if len(closeCode) > 0 {
			code = closeCode[0]
		}

		ws.printWrite(typ, content, code)

		content = websocket.FormatCloseMessage(code, string(content))

	default:
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				fmt.Errorf("unexpected websocket message type %s",
					wsMessageType(typ)),
			},
		})
		return
	}

	if !ws.setWriteDeadline(opChain) {
		return
	}

	if err := ws.conn.WriteMessage(typ, content); err != nil {
		opChain.fail(AssertionFailure{
			Type: AssertOperation,
			Errors: []error{
				errors.New("failed to write to websocket"),
				err,
			},
		})
		return
	}
}

func (ws *Websocket) setReadDeadline(opChain *chain) bool {
	deadline := infiniteTime
	if ws.readTimeout != noDuration {
		deadline = time.Now().Add(ws.readTimeout)
	}

	if err := ws.conn.SetReadDeadline(deadline); err != nil {
		opChain.fail(AssertionFailure{
			Type: AssertOperation,
			Errors: []error{
				errors.New("failed to set read deadline for websocket"),
				err,
			},
		})
		return false
	}

	return true
}

func (ws *Websocket) setWriteDeadline(opChain *chain) bool {
	deadline := infiniteTime
	if ws.writeTimeout != noDuration {
		deadline = time.Now().Add(ws.writeTimeout)
	}

	if err := ws.conn.SetWriteDeadline(deadline); err != nil {
		opChain.fail(AssertionFailure{
			Type: AssertOperation,
			Errors: []error{
				errors.New("failed to set write deadline for websocket"),
				err,
			},
		})
		return false
	}

	return true
}

func (ws *Websocket) printRead(typ int, content []byte, closeCode int) {
	for _, printer := range ws.config.Printers {
		if p, ok := printer.(WebsocketPrinter); ok {
			p.WebsocketRead(typ, content, closeCode)
		}
	}
}

func (ws *Websocket) printWrite(typ int, content []byte, closeCode int) {
	for _, printer := range ws.config.Printers {
		if p, ok := printer.(WebsocketPrinter); ok {
			p.WebsocketWrite(typ, content, closeCode)
		}
	}
}
