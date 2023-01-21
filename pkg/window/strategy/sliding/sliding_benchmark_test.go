package sliding

import (
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/window/keyed"
)

func BenchmarkSliding_AssignWindow(b *testing.B) {
	AssignWindowHelper(b)
	b.ReportAllocs()
}

func BenchmarkSliding_InsertIfNotPresent(b *testing.B) {
	InsertWindowHelper(b)
	b.ReportAllocs()
}

func BenchmarkSliding_RemoveWindowsWithCheck(b *testing.B) {
	RemoveWindowWithCheckHelper(b)
	b.ReportAllocs()
}

func BenchmarkSliding_RemoveWindowsPerBatch(b *testing.B) {
	RemoveWindowPerBatchHelper(b)
	b.ReportAllocs()
}

func InsertWindowHelper(b *testing.B) {
	b.Helper()
	var (
		//msgCount  = 100
		eventTime = time.Unix(60, 0)
		winLength = time.Second * 600
		sliding   = time.Second * 60
	)
	windowStrat := NewSliding(winLength, sliding)

	for i := 0; i < b.N; i++ {
		window := keyed.NewKeyedWindow(eventTime, eventTime.Add(winLength))
		windowStrat.InsertIfNotPresent(window)
		eventTime = eventTime.Add(winLength)
	}

}

func RemoveWindowWithCheckHelper(b *testing.B) {
	b.Helper()
	var (
		length    = 10000
		eventTime = time.Unix(60, 0)
		winLength = time.Second * 600
		sliding   = time.Second * 60
	)
	windowStrat := NewSliding(winLength, sliding)

	b.StopTimer()
	for i := 0; i < length; i++ {
		window := keyed.NewKeyedWindow(eventTime, eventTime.Add(winLength))
		eventTime = eventTime.Add(winLength)
		windowStrat.InsertIfNotPresent(window)
	}
	b.StartTimer()

	currentWatermark := time.Unix(60, 0)
	batchSize := 500
	batchCount := 0
	latestWatermark := eventTime
	for i := 0; i < b.N; i++ {
		if currentWatermark.After(latestWatermark) {
			_ = windowStrat.RemoveWindows(eventTime)
			latestWatermark = eventTime
		}
		// update watermark once per batch
		if batchCount == batchSize {
			currentWatermark = currentWatermark.Add(winLength)
		}
		batchCount += 1
	}

}

func RemoveWindowPerBatchHelper(b *testing.B) {
	b.Helper()
	var (
		length    = 10000
		eventTime = time.Unix(60, 0)
		winLength = time.Second * 600
		sliding   = time.Second * 60
	)
	windowStrat := NewSliding(winLength, sliding)

	b.StopTimer()
	for i := 0; i < length; i++ {
		window := keyed.NewKeyedWindow(eventTime, eventTime.Add(winLength))
		eventTime = eventTime.Add(winLength)
		windowStrat.InsertIfNotPresent(window)
	}
	b.StartTimer()

	batchSize := 100
	batchCount := 0
	currentWatermark := time.Unix(60, 0)
	for i := 0; i < b.N; i++ {
		// invoke remove windows per batch
		if batchCount == batchSize {
			_ = windowStrat.RemoveWindows(currentWatermark)
		}
		// update watermark once in five batches
		if batchCount == 5*batchSize {
			currentWatermark = currentWatermark.Add(time.Second)
		}
		batchCount += 1
	}

}

func AssignWindowHelper(b *testing.B) {
	b.Helper()
	var (
		//msgCount  = 100
		eventTime = time.Unix(60, 0)
		winLength = time.Second * 600
		sliding   = time.Second * 60
	)
	windowStrat := NewSliding(winLength, sliding)

	for i := 0; i < b.N; i++ {
		windowStrat.AssignWindow(eventTime)
		eventTime.Add(winLength)
	}
}
