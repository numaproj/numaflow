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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEntity(t *testing.T) {
	e := NewProcessorEntity("pod0")
	assert.Equal(t, "pod0", e.GetID())
}

func ExampleWatermark_String() {
	location, _ := time.LoadLocation("UTC")
	wm := Watermark(time.Unix(1651129200, 0).In(location))
	fmt.Println(wm)
	// output:
	// 2022-04-28T07:00:00Z
}

func TestExampleWatermarkUnix(t *testing.T) {
	wm := Watermark(time.UnixMilli(1651129200000))
	assert.Equal(t, int64(1651129200000), wm.UnixMilli())
}
