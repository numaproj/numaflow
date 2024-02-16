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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
)

func TestNewIdleManager(t *testing.T) {
	var (
		toBufferName = "testBuffer"
		o            = isb.SimpleIntOffset(func() int64 {
			return int64(100)
		})
	)
	idleManager := NewIdleManager(10)
	assert.NotNil(t, idleManager)
	assert.True(t, idleManager.NeedToSendCtrlMsg(toBufferName))
	idleManager.Update(0, toBufferName, o)
	getOffset := idleManager.Get(toBufferName)
	assert.Equal(t, o.String(), getOffset.String())
	assert.False(t, idleManager.NeedToSendCtrlMsg(toBufferName))
	idleManager.Reset(0, toBufferName)
	assert.True(t, idleManager.NeedToSendCtrlMsg(toBufferName))
}
