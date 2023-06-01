package udferr

import (
	"fmt"
)

// ErrKind represents if the error is retryable
type ErrKind int16

const (
	Retryable    ErrKind = iota // The error is retryable
	NonRetryable                // The error is non-retryable
	Unknown                     // Unknown err kind
)

func (ek ErrKind) String() string {
	switch ek {
	case Retryable:
		return "Retryable"
	case NonRetryable:
		return "NonRetryable"
	case Unknown:
		return "Unknown"
	default:
		return "Unknown"
	}
}

// UDFError is returned to the main numaflow indicates the status of the error
type UDFError struct {
	errKind    ErrKind
	errMessage string
}

func New(kind ErrKind, msg string) *UDFError {
	return &UDFError{
		errKind:    kind,
		errMessage: msg,
	}
}

func (e *UDFError) Error() string {
	return fmt.Sprintf("%s: %s", e.errKind, e.errMessage)
}

func (e *UDFError) ErrorKind() ErrKind {
	return e.errKind
}

func (e *UDFError) ErrorMessage() string {
	return e.errMessage
}

// FromError gets error information from the UDFError
func FromError(err error) (udfErr *UDFError, ok bool) {
	if err == nil {
		return nil, true
	}
	if se, ok := err.(interface {
		ErrorKind() ErrKind
		ErrorMessage() string
	}); ok {
		return &UDFError{se.ErrorKind(), se.ErrorMessage()}, true
	}
	return &UDFError{Unknown, err.Error()}, false
}
