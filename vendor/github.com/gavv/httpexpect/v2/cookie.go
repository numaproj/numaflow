package httpexpect

import (
	"errors"
	"net/http"
	"time"
)

// Cookie provides methods to inspect attached http.Cookie value.
type Cookie struct {
	noCopy noCopy
	chain  *chain
	value  *http.Cookie
}

// NewCookie returns a new Cookie instance.
//
// If reporter is nil, the function panics.
// If value is nil, failure is reported.
//
// Example:
//
//	cookie := NewCookie(t, &http.Cookie{...})
//
//	cookie.Domain().IsEqual("example.com")
//	cookie.Path().IsEqual("/")
//	cookie.Expires().InRange(time.Now(), time.Now().Add(time.Hour * 24))
func NewCookie(reporter Reporter, value *http.Cookie) *Cookie {
	return newCookie(newChainWithDefaults("Cookie()", reporter), value)
}

// NewCookieC returns a new Cookie instance with config.
//
// Requirements for config are same as for WithConfig function.
// If value is nil, failure is reported.
//
// See NewCookie for usage example.
func NewCookieC(config Config, value *http.Cookie) *Cookie {
	return newCookie(newChainWithConfig("Cookie()", config.withDefaults()), value)
}

func newCookie(parent *chain, val *http.Cookie) *Cookie {
	c := &Cookie{chain: parent.clone(), value: nil}

	opChain := c.chain.enter("")
	defer opChain.leave()

	if val == nil {
		opChain.fail(AssertionFailure{
			Type:   AssertNotNil,
			Actual: &AssertionValue{val},
			Errors: []error{
				errors.New("expected: non-nil cookie"),
			},
		})
	} else {
		c.value = val
	}

	return c
}

// Raw returns underlying http.Cookie value attached to Cookie.
// This is the value originally passed to NewCookie.
//
// Example:
//
//	cookie := NewCookie(t, c)
//	assert.Equal(t, c, cookie.Raw())
func (c *Cookie) Raw() *http.Cookie {
	return c.value
}

// Alias is similar to Value.Alias.
func (c *Cookie) Alias(name string) *Cookie {
	opChain := c.chain.enter("Alias(%q)", name)
	defer opChain.leave()

	c.chain.setAlias(name)
	return c
}

// Name returns a new String instance with cookie name.
//
// Example:
//
//	cookie := NewCookie(t, &http.Cookie{...})
//	cookie.Name().IsEqual("session")
func (c *Cookie) Name() *String {
	opChain := c.chain.enter("Name()")
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	return newString(opChain, c.value.Name)
}

// Value returns a new String instance with cookie value.
//
// Example:
//
//	cookie := NewCookie(t, &http.Cookie{...})
//	cookie.Value().IsEqual("gH6z7Y")
func (c *Cookie) Value() *String {
	opChain := c.chain.enter("Value()")
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	return newString(opChain, c.value.Value)
}

// Domain returns a new String instance with cookie domain.
//
// Example:
//
//	cookie := NewCookie(t, &http.Cookie{...})
//	cookie.Domain().IsEqual("example.com")
func (c *Cookie) Domain() *String {
	opChain := c.chain.enter("Domain()")
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	return newString(opChain, c.value.Domain)
}

// Path returns a new String instance with cookie path.
//
// Example:
//
//	cookie := NewCookie(t, &http.Cookie{...})
//	cookie.Path().IsEqual("/foo")
func (c *Cookie) Path() *String {
	opChain := c.chain.enter("Path()")
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	return newString(opChain, c.value.Path)
}

// Expires returns a new DateTime instance with cookie expiration date.
//
// Example:
//
//	cookie := NewCookie(t, &http.Cookie{...})
//	cookie.Expires().InRange(time.Now(), time.Now().Add(time.Hour * 24))
func (c *Cookie) Expires() *DateTime {
	opChain := c.chain.enter("Expires()")
	defer opChain.leave()

	if opChain.failed() {
		return newDateTime(opChain, time.Unix(0, 0))
	}

	return newDateTime(opChain, c.value.Expires)
}

// HasMaxAge succeeds if cookie has Max-Age field.
//
// In particular, if Max-Age is present and is zero (which means delete
// cookie now), method succeeds.
//
// Example:
//
//	cookie := NewCookie(t, &http.Cookie{...})
//	cookie.HasMaxAge()
func (c *Cookie) HasMaxAge() *Cookie {
	opChain := c.chain.enter("HasMaxAge()")
	defer opChain.leave()

	if opChain.failed() {
		return c
	}

	if c.value.MaxAge == 0 {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{c.value},
			Errors: []error{
				errors.New("expected: cookie has Max-Age field"),
			},
		})
	}

	return c
}

// NotHasMaxAge succeeds if cookie does not have Max-Age field.
//
// In particular, if Max-Age is present and is zero (which means delete
// cookie now), method fails.
//
// Example:
//
//	cookie := NewCookie(t, &http.Cookie{...})
//	cookie.NotHasMaxAge()
func (c *Cookie) NotHasMaxAge() *Cookie {
	opChain := c.chain.enter("NotHasMaxAge()")
	defer opChain.leave()

	if opChain.failed() {
		return c
	}

	if c.value.MaxAge != 0 {
		opChain.fail(AssertionFailure{
			Type:   AssertNotValid,
			Actual: &AssertionValue{c.value},
			Errors: []error{
				errors.New("expected: cookie does not have Max-Age field"),
			},
		})
	}

	return c
}

// Deprecated: use HasMaxAge instead.
func (c *Cookie) HaveMaxAge() *Cookie {
	return c.HasMaxAge()
}

// Deprecated: use NotHasMaxAge instead.
func (c *Cookie) NotHaveMaxAge() *Cookie {
	return c.NotHasMaxAge()
}

// MaxAge returns a new Duration instance with cookie Max-Age field.
//
// If Max-Age is not present, method fails.
//
// If Max-Age is present and is zero (which means delete cookie now),
// methods succeeds and the returned Duration is equal to zero.
//
// Example:
//
//	cookie := NewCookie(t, &http.Cookie{...})
//	cookie.HasMaxAge()
//	cookie.MaxAge().InRange(time.Minute, time.Minute*10)
func (c *Cookie) MaxAge() *Duration {
	opChain := c.chain.enter("MaxAge()")
	defer opChain.leave()

	if opChain.failed() {
		return newDuration(opChain, nil)
	}

	switch {
	case c.value.MaxAge == 0: // zero value means not present
		// TODO: after removing Duration.IsSet, add failure here (breaking change)
		_ = (*Duration).IsSet
		return newDuration(opChain, nil)

	case c.value.MaxAge < 0: // negative value means present and zero
		age := time.Duration(0)
		return newDuration(opChain, &age)

	default:
		age := time.Duration(c.value.MaxAge) * time.Second
		return newDuration(opChain, &age)
	}
}
