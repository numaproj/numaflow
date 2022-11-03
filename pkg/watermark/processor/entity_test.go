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
