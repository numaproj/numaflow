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

package noop

import (
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal"
)

// noopWAL is a no-op pbq store which does not do any operation but can be safely invoked.
type noopWAL struct {
}

var _ wal.WAL = (*noopWAL)(nil)

func NewPBQNoOpStore() (wal.WAL, error) {
	return &noopWAL{}, nil
}

func (p *noopWAL) Replay() (<-chan *isb.ReadMessage, <-chan error) {
	return nil, nil
}

func (p *noopWAL) Write(msg *isb.ReadMessage) error {
	return nil
}

func (p *noopWAL) Close() error {
	return nil
}

func (p *noopWAL) PartitionID() partition.ID {
	return partition.ID{}
}
