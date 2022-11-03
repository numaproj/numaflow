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

package cat

import (
	"context"
	"testing"
	"time"

	funcsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/stretchr/testify/assert"
)

type testDatum struct {
	value     []byte
	eventTime time.Time
	watermark time.Time
}

func (h *testDatum) Value() []byte {
	return h.value
}

func (h *testDatum) EventTime() time.Time {
	return h.eventTime
}

func (h *testDatum) Watermark() time.Time {
	return h.watermark
}

func TestNew(t *testing.T) {
	ctx := context.Background()
	p := New()
	req := []byte{0}
	messages := p(ctx, "", &testDatum{
		value:     req,
		eventTime: time.Time{},
		watermark: time.Time{},
	})
	assert.Equal(t, 1, len(messages))
	assert.Equal(t, funcsdk.ALL, messages[0].Key)
	assert.Equal(t, req, messages[0].Value)
}
