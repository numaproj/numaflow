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

package udsource

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
)

type userDefinedSource struct {
	udsource *UDSgRPCBasedUDSource
}

func New(udsource *UDSgRPCBasedUDSource) (*userDefinedSource, error) {
	return &userDefinedSource{
		udsource: udsource,
	}, nil
}

func (u *userDefinedSource) GetName() string {
	return "UserDefinedSource"
}

// GetPartitionIdx returns the partition number for the user-defined source.
// Source is like a buffer with only one partition. So, we always return 0
func (u *userDefinedSource) GetPartitionIdx() int32 {
	return 0
}

func (u *userDefinedSource) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	// TODO(udsource) - Implement it
	return nil, nil
}

func (u *userDefinedSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	// TODO(udsource) - Implement it
	return make([]error, len(offsets))
}

func (u *userDefinedSource) Pending(_ context.Context) (int64, error) {
	// TODO(udsource) - Implement it
	return 0, nil
}

func (u *userDefinedSource) NoAck(_ context.Context, _ []isb.Offset) {
	// User defined source does not support NoAck
	panic("not implemented")
}

func (u *userDefinedSource) Close() error {
	// TODO(udsource) - Implement it
	return nil
}

func (u *userDefinedSource) Stop() {
	// TODO(udsource) - Implement it
}

func (u *userDefinedSource) ForceStop() {
	// TODO(udsource) - Implement it
}

func (u *userDefinedSource) Start() <-chan struct{} {
	// TODO(udsource) - Implement it
	return nil
}
