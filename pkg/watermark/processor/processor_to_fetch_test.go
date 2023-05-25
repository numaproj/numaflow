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

package processor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromProcessor_setStatus(t *testing.T) {
	var ctx = context.Background()
	p := NewProcessorToFetch(ctx, NewProcessorEntity("test-pod"), 5)
	p.setStatus(_inactive)
	assert.Equal(t, _inactive, p.status)
}
