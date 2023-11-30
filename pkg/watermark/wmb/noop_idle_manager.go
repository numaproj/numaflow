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

package wmb

import (
	"github.com/numaproj/numaflow/pkg/isb"
)

type noOpIdleManager struct {
}

// NewNoOpIdleManager returns an no op idleManager object as the IdleManager Interface type
func NewNoOpIdleManager() IdleManager {
	return &noOpIdleManager{}
}

func (n *noOpIdleManager) NeedToSendCtrlMsg(string) bool {
	// no op idle manager won't write any ctrl message
	// so always return false
	return false
}

func (n *noOpIdleManager) Get(string) isb.Offset {
	return isb.SimpleIntOffset(func() int64 { return int64(-1) })
}

func (n *noOpIdleManager) Update(string, isb.Offset) {
}

func (n *noOpIdleManager) Reset(string) {
}
