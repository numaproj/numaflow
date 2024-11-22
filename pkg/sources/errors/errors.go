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

package errors

// SourceReadErr represents any source read related error
type SourceReadErr struct {
	Message   string
	Retryable bool
}

func (e *SourceReadErr) Error() string {
	return e.Message
}

func (e *SourceReadErr) Is(target error) bool {
	return target.Error() == e.Error()
}

// IsRetryable is true if the error is retryable
func (e *SourceReadErr) IsRetryable() bool {
	return e.Retryable
}

type SourceAckErr struct {
	Message   string
	Retryable bool
}

func (e *SourceAckErr) Error() string {
	return e.Message
}

func (e *SourceAckErr) Is(target error) bool {
	return target.Error() == e.Error()
}

// IsRetryable is true if the error is retryable
func (e *SourceAckErr) IsRetryable() bool {
	return e.Retryable
}

type SourcePendingErr struct {
	Message   string
	Retryable bool
}

func (e *SourcePendingErr) Error() string {
	return e.Message
}

func (e *SourcePendingErr) Is(target error) bool {
	return target.Error() == e.Error()
}

// IsRetryable is true if the error is retryable
func (e *SourcePendingErr) IsRetryable() bool {
	return e.Retryable
}
