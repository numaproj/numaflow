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

package fetch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessorHeartbeat(t *testing.T) {
	hb := newProcessorHeartbeat()
	hb.put("pod1", 1)
	assert.Equal(t, int64(1), hb.get("pod1"))
	hb.put("pod1", 5)
	assert.Equal(t, int64(5), hb.get("pod1"))
	hb.put("pod2", 6)
	assert.Equal(t, map[string]int64{"pod1": int64(5), "pod2": int64(6)}, hb.getAll())
	hb.delete("pod1")
	assert.Equal(t, map[string]int64{"pod2": int64(6)}, hb.getAll())
}
