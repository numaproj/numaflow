package httpexpect

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gorilla/websocket"
)

// WebsocketMessage provides methods to inspect message read from WebSocket connection.
type WebsocketMessage struct {
	noCopy noCopy
	chain  *chain

	typ       int
	content   []byte
	closeCode int
}

// NewWebsocketMessage returns a new WebsocketMessage instance.
//
// If reporter is nil, the function panics.
// Content may be nil.
//
// Example:
//
//	m := NewWebsocketMessage(t, websocket.TextMessage, []byte("content"), 0)
//	m.TextMessage()
func NewWebsocketMessage(
	reporter Reporter, typ int, content []byte, closeCode ...int,
) *WebsocketMessage {
	return newWebsocketMessage(
		newChainWithDefaults("WebsocketMessage()", reporter),
		typ,
		content,
		closeCode...,
	)
}

// NewWebsocketMessageC returns a new WebsocketMessage instance with config.
//
// Requirements for config are same as for WithConfig function.
// Content may be nil.
//
// Example:
//
//	m := NewWebsocketMessageC(config, websocket.TextMessage, []byte("content"), 0)
//	m.TextMessage()
func NewWebsocketMessageC(
	config Config, typ int, content []byte, closeCode ...int,
) *WebsocketMessage {
	return newWebsocketMessage(
		newChainWithConfig("WebsocketMessage()", config.withDefaults()),
		typ,
		content,
		closeCode...,
	)
}

func newWebsocketMessage(
	parent *chain, typ int, content []byte, closeCode ...int,
) *WebsocketMessage {
	wm := newEmptyWebsocketMessage(parent)

	opChain := wm.chain.enter("")
	defer opChain.leave()

	wm.typ = typ
	wm.content = content

	if len(closeCode) > 1 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple closeCode arguments"),
			},
		})
		return wm
	}

	if len(closeCode) != 0 {
		wm.closeCode = closeCode[0]
	}

	return wm
}

func newEmptyWebsocketMessage(parent *chain) *WebsocketMessage {
	return &WebsocketMessage{
		chain: parent.clone(),
	}
}

// Raw returns underlying type, content and close code of WebSocket message.
// Theses values are originally read from WebSocket connection.
func (wm *WebsocketMessage) Raw() (typ int, content []byte, closeCode int) {
	return wm.typ, wm.content, wm.closeCode
}

// Alias is similar to Value.Alias.
func (wm *WebsocketMessage) Alias(name string) *WebsocketMessage {
	opChain := wm.chain.enter("Alias(%q)", name)
	defer opChain.leave()

	wm.chain.setAlias(name)
	return wm
}

// CloseMessage is a shorthand for m.Type(websocket.CloseMessage).
func (wm *WebsocketMessage) CloseMessage() *WebsocketMessage {
	opChain := wm.chain.enter("CloseMessage()")
	defer opChain.leave()

	wm.checkType(opChain, websocket.CloseMessage)

	return wm
}

// NotCloseMessage is a shorthand for m.NotType(websocket.CloseMessage).
func (wm *WebsocketMessage) NotCloseMessage() *WebsocketMessage {
	opChain := wm.chain.enter("NotCloseMessage()")
	defer opChain.leave()

	wm.checkNotType(opChain, websocket.CloseMessage)

	return wm
}

// BinaryMessage is a shorthand for m.Type(websocket.BinaryMessage).
func (wm *WebsocketMessage) BinaryMessage() *WebsocketMessage {
	opChain := wm.chain.enter("BinaryMessage()")
	defer opChain.leave()

	wm.checkType(opChain, websocket.BinaryMessage)

	return wm
}

// NotBinaryMessage is a shorthand for m.NotType(websocket.BinaryMessage).
func (wm *WebsocketMessage) NotBinaryMessage() *WebsocketMessage {
	opChain := wm.chain.enter("NotBinaryMessage()")
	defer opChain.leave()

	wm.checkNotType(opChain, websocket.BinaryMessage)

	return wm
}

// TextMessage is a shorthand for m.Type(websocket.TextMessage).
func (wm *WebsocketMessage) TextMessage() *WebsocketMessage {
	opChain := wm.chain.enter("TextMessage()")
	defer opChain.leave()

	wm.checkType(opChain, websocket.TextMessage)

	return wm
}

// NotTextMessage is a shorthand for m.NotType(websocket.TextMessage).
func (wm *WebsocketMessage) NotTextMessage() *WebsocketMessage {
	opChain := wm.chain.enter("NotTextMessage()")
	defer opChain.leave()

	wm.checkNotType(opChain, websocket.TextMessage)

	return wm
}

// Type succeeds if WebSocket message type is one of the given.
//
// WebSocket message types are defined in RFC 6455, section 11.8.
// See also https://godoc.org/github.com/gorilla/websocket#pkg-constants
//
// Example:
//
//	msg := conn.Expect()
//	msg.Type(websocket.TextMessage, websocket.BinaryMessage)
func (wm *WebsocketMessage) Type(types ...int) *WebsocketMessage {
	opChain := wm.chain.enter("Type()")
	defer opChain.leave()

	wm.checkType(opChain, types...)

	return wm
}

// NotType succeeds if WebSocket message type is none of the given.
//
// WebSocket message types are defined in RFC 6455, section 11.8.
// See also https://godoc.org/github.com/gorilla/websocket#pkg-constants
//
// Example:
//
//	msg := conn.Expect()
//	msg.NotType(websocket.CloseMessage, websocket.BinaryMessage)
func (wm *WebsocketMessage) NotType(types ...int) *WebsocketMessage {
	opChain := wm.chain.enter("NotType()")
	defer opChain.leave()

	wm.checkNotType(opChain, types...)

	return wm
}

func (wm *WebsocketMessage) checkType(opChain *chain, types ...int) {
	if opChain.failed() {
		return
	}

	if len(types) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("missing type argument"),
			},
		})
		return
	}

	found := false
	for _, t := range types {
		if t == wm.typ {
			found = true
			break
		}
	}

	if !found {
		if len(types) == 1 {
			opChain.fail(AssertionFailure{
				Type:     AssertEqual,
				Actual:   &AssertionValue{wsMessageType(wm.typ)},
				Expected: &AssertionValue{wsMessageType(types[0])},
				Errors: []error{
					errors.New("expected: message types are equal"),
				},
			})
		} else {
			typeList := make([]interface{}, 0, len(types))
			for _, t := range types {
				typeList = append(typeList, wsMessageType(t))
			}

			opChain.fail(AssertionFailure{
				Type:     AssertBelongs,
				Actual:   &AssertionValue{wsMessageType(wm.typ)},
				Expected: &AssertionValue{AssertionList(typeList)},
				Errors: []error{
					errors.New("expected: message type belongs to given list"),
				},
			})
		}
	}
}

func (wm *WebsocketMessage) checkNotType(opChain *chain, types ...int) {
	if opChain.failed() {
		return
	}

	if len(types) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("missing type argument"),
			},
		})
		return
	}

	found := false
	for _, t := range types {
		if t == wm.typ {
			found = true
			break
		}
	}

	if found {
		if len(types) == 1 {
			opChain.fail(AssertionFailure{
				Type:     AssertNotEqual,
				Actual:   &AssertionValue{wsMessageType(wm.typ)},
				Expected: &AssertionValue{wsMessageType(types[0])},
				Errors: []error{
					errors.New("expected: message types are non-equal"),
				},
			})
		} else {
			typeList := make([]interface{}, 0, len(types))
			for _, t := range types {
				typeList = append(typeList, wsMessageType(t))
			}

			opChain.fail(AssertionFailure{
				Type:     AssertNotBelongs,
				Actual:   &AssertionValue{wsMessageType(wm.typ)},
				Expected: &AssertionValue{AssertionList(typeList)},
				Errors: []error{
					errors.New("expected: message type does not belong to given list"),
				},
			})
		}
	}
}

// Code succeeds if WebSocket close code is one of the given.
//
// Code fails if WebSocket message type is not "8 - Connection Close Frame".
//
// WebSocket close codes are defined in RFC 6455, section 11.7.
// See also https://godoc.org/github.com/gorilla/websocket#pkg-constants
//
// Example:
//
//	msg := conn.Expect().Closed()
//	msg.Code(websocket.CloseNormalClosure, websocket.CloseGoingAway)
func (wm *WebsocketMessage) Code(codes ...int) *WebsocketMessage {
	opChain := wm.chain.enter("Code()")
	defer opChain.leave()

	wm.checkCode(opChain, codes...)

	return wm
}

// NotCode succeeds if WebSocket close code is none of the given.
//
// NotCode fails if WebSocket message type is not "8 - Connection Close Frame".
//
// WebSocket close codes are defined in RFC 6455, section 11.7.
// See also https://godoc.org/github.com/gorilla/websocket#pkg-constants
//
// Example:
//
//	msg := conn.Expect().Closed()
//	msg.NotCode(websocket.CloseAbnormalClosure, websocket.CloseNoStatusReceived)
func (wm *WebsocketMessage) NotCode(codes ...int) *WebsocketMessage {
	opChain := wm.chain.enter("NotCode()")
	defer opChain.leave()

	wm.checkNotCode(opChain, codes...)

	return wm
}

func (wm *WebsocketMessage) checkCode(opChain *chain, codes ...int) {
	if opChain.failed() {
		return
	}

	if len(codes) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("missing code argument"),
			},
		})
		return
	}

	if wm.typ != websocket.CloseMessage {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{wsMessageType(wm.typ)},
			Expected: &AssertionValue{wsMessageType(websocket.CloseMessage)},
			Errors: []error{
				errors.New("expected: close message"),
			},
		})
		return
	}

	found := false
	for _, c := range codes {
		if c == wm.closeCode {
			found = true
			break
		}
	}

	if !found {
		if len(codes) == 1 {
			opChain.fail(AssertionFailure{
				Type:     AssertEqual,
				Actual:   &AssertionValue{wsCloseCode(wm.closeCode)},
				Expected: &AssertionValue{wsCloseCode(codes[0])},
				Errors: []error{
					errors.New("expected: close codes are equal"),
				},
			})
		} else {
			codeList := make([]interface{}, 0, len(codes))
			for _, c := range codes {
				codeList = append(codeList, wsCloseCode(c))
			}

			opChain.fail(AssertionFailure{
				Type:     AssertBelongs,
				Actual:   &AssertionValue{wsCloseCode(wm.closeCode)},
				Expected: &AssertionValue{AssertionList(codeList)},
				Errors: []error{
					errors.New("expected: close code belongs to given list"),
				},
			})
		}
	}
}

func (wm *WebsocketMessage) checkNotCode(opChain *chain, codes ...int) {
	if opChain.failed() {
		return
	}

	if len(codes) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("missing code argument"),
			},
		})
		return
	}

	if wm.typ != websocket.CloseMessage {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{wsMessageType(wm.typ)},
			Expected: &AssertionValue{wsMessageType(websocket.CloseMessage)},
			Errors: []error{
				errors.New("expected: close message"),
			},
		})
		return
	}

	found := false
	for _, c := range codes {
		if c == wm.closeCode {
			found = true
			break
		}
	}

	if found {
		if len(codes) == 1 {
			opChain.fail(AssertionFailure{
				Type:     AssertNotEqual,
				Actual:   &AssertionValue{wsCloseCode(wm.closeCode)},
				Expected: &AssertionValue{wsCloseCode(codes[0])},
				Errors: []error{
					errors.New("expected: close codes are non-equal"),
				},
			})
		} else {
			codeList := make([]interface{}, 0, len(codes))
			for _, c := range codes {
				codeList = append(codeList, wsCloseCode(c))
			}

			opChain.fail(AssertionFailure{
				Type:     AssertNotBelongs,
				Actual:   &AssertionValue{wsCloseCode(wm.closeCode)},
				Expected: &AssertionValue{AssertionList(codeList)},
				Errors: []error{
					errors.New("expected: close code dose not belong to given list"),
				},
			})
		}
	}
}

// NoContent succeeds if WebSocket message has no content (is empty).
func (wm *WebsocketMessage) NoContent() *WebsocketMessage {
	opChain := wm.chain.enter("NoContent()")
	defer opChain.leave()

	if opChain.failed() {
		return wm
	}

	if !(len(wm.content) == 0) {
		var actual interface{}
		switch wm.typ {
		case websocket.BinaryMessage:
			actual = wm.content

		default:
			actual = string(wm.content)
		}

		opChain.fail(AssertionFailure{
			Type:   AssertEmpty,
			Actual: &AssertionValue{actual},
			Errors: []error{
				errors.New("expected: message content is empty"),
			},
		})
	}

	return wm
}

// Body returns a new String instance with WebSocket message content.
//
// Example:
//
//	msg := conn.Expect()
//	msg.Body().NotEmpty()
//	msg.Body().Length().IsEqual(100)
func (wm *WebsocketMessage) Body() *String {
	opChain := wm.chain.enter("Body()")
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	return newString(opChain, string(wm.content))
}

// JSON returns a new Value instance with JSON contents of WebSocket message.
//
// JSON succeeds if JSON may be decoded from message content.
//
// Example:
//
//	msg := conn.Expect()
//	msg.JSON().Array().ConsistsOf("foo", "bar")
func (wm *WebsocketMessage) JSON() *Value {
	opChain := wm.chain.enter("JSON()")
	defer opChain.leave()

	if opChain.failed() {
		return newValue(opChain, nil)
	}

	var value interface{}

	if err := json.Unmarshal(wm.content, &value); err != nil {
		opChain.fail(AssertionFailure{
			Type: AssertValid,
			Actual: &AssertionValue{
				string(wm.content),
			},
			Errors: []error{
				errors.New("failed to decode json"),
				err,
			},
		})
		return newValue(opChain, nil)
	}

	return newValue(opChain, value)
}

type wsMessageType int

func (wmt wsMessageType) String() string {
	s := "unknown"

	switch wmt {
	case websocket.TextMessage:
		s = "text"
	case websocket.BinaryMessage:
		s = "binary"
	case websocket.CloseMessage:
		s = "close"
	case websocket.PingMessage:
		s = "ping"
	case websocket.PongMessage:
		s = "pong"
	}

	return fmt.Sprintf("%s(%d)", s, wmt)
}

type wsCloseCode int

// https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent/code
func (wcc wsCloseCode) String() string {
	s := "Unknown"

	switch wcc {
	case 1000:
		s = "NormalClosure"
	case 1001:
		s = "GoingAway"
	case 1002:
		s = "ProtocolError"
	case 1003:
		s = "UnsupportedData"
	case 1004:
		s = "Reserved"
	case 1005:
		s = "NoStatusReceived"
	case 1006:
		s = "AbnormalClosure"
	case 1007:
		s = "InvalidFramePayloadData"
	case 1008:
		s = "PolicyViolation"
	case 1009:
		s = "MessageTooBig"
	case 1010:
		s = "MandatoryExtension"
	case 1011:
		s = "InternalServerError"
	case 1012:
		s = "ServiceRestart"
	case 1013:
		s = "TryAgainLater"
	case 1014:
		s = "BadGateway"
	case 1015:
		s = "TLSHandshake"
	}

	return fmt.Sprintf("%s(%d)", s, wcc)
}
