/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package error

import (
	"fmt"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrKind represents if the error is retryable
type ErrKind int16

const (
	Retryable    ErrKind = iota // The error is retryable
	NonRetryable                // The error is non-retryable
	Canceled                    // Request canceled
	Unknown                     // Unknown err kind
)

func (ek ErrKind) String() string {
	switch ek {
	case Retryable:
		return "Retryable"
	case NonRetryable:
		return "NonRetryable"
	case Canceled:
		return "Canceled"
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

// ToUDFErr converts gRPC error to UDF Error
func ToUDFErr(name string, err error) error {
	if err == nil {
		return nil
	}
	statusCode, ok := status.FromError(err)
	// default udfError
	udfError := New(NonRetryable, statusCode.Message())
	// check if it's a standard status code
	if !ok {
		// if not, the status code will be unknown which we consider as non retryable
		// return default udfError
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	}
	switch statusCode.Code() {
	case codes.OK:
		return nil
	case codes.DeadlineExceeded, codes.Unavailable, codes.Unknown:
		// update to retryable err
		udfError = New(Retryable, statusCode.Message())
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	case codes.Canceled:
		udfError = New(Canceled, statusCode.Message())
		return udfError
	default:
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	}
}
