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

package udsink

import "fmt"

// ApplyUDSinkErr represents any UDSink related error
type ApplyUDSinkErr struct {
	UserUDSinkErr bool
	Message       string
	InternalErr
}

// InternalErr represents errors internal to the platform
type InternalErr struct {
	Flag        bool
	MainCarDown bool
}

// IsUserUDSinkErr is true if the problem is due to the user code in the UDSink.
func (e ApplyUDSinkErr) IsUserUDSinkErr() bool {
	return e.UserUDSinkErr
}

// IsInternalErr is true if this is a platform issue. This is a blocking error.
func (e ApplyUDSinkErr) IsInternalErr() bool {
	return e.InternalErr.Flag
}

func (e ApplyUDSinkErr) Error() string {
	return fmt.Sprint(e.Message)
}
