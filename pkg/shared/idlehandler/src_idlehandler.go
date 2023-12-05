package idlehandler

import (
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// Questions to ask when publishing idle watermark:
// 1. How to decide if the source is idling? -> Answer: if I am not reading any messages for WatermarkConfig.Threshold(provided by user) time.
// 2. When to publish the idle watermark? -> Answer: if the source is idling and the step interval has passed.
// 3. What to publish as the idle watermark? -> Answer: the current watermark + WatermarkConfig.IncrementBy(provided by user).

// SrcIdleHandler handles operations related to idle watermarks for source.
type SrcIdleHandler struct {
	config                  *dfv1.Watermark
	lastIdleWmPublishedTime time.Time
	updatedTS               time.Time
	wmFetcher               fetch.SourceFetcher
	srcPublisher            publish.SourcePublisher
}

// NewSrcIdleHandler creates a new instance of SrcIdleHandler.
func NewSrcIdleHandler(config *dfv1.Watermark, fetcher fetch.SourceFetcher, publisher publish.SourcePublisher) *SrcIdleHandler {
	return &SrcIdleHandler{
		config:       config,
		wmFetcher:    fetcher,
		srcPublisher: publisher,
		updatedTS:    time.Now(),
	}
}

// isSourceIdling checks if the source is idling by comparing the last updated timestamp with the threshold.
func (iw *SrcIdleHandler) isSourceIdling() bool {
	// if the source is not configured for idling, return false
	if iw.config == nil || iw.config.IdleSource == nil {
		return false
	}

	// if the threshold has not passed, return false
	if time.Since(iw.updatedTS) < iw.config.IdleSource.GetThreshold() {
		return false
	}
	return true
}

// hasStepIntervalPassed verifies if the step interval has passed.
func (iw *SrcIdleHandler) hasStepIntervalPassed() bool {
	return time.Since(iw.lastIdleWmPublishedTime) >= iw.config.IdleSource.GetStepInterval()
}

// PublishSrcIdleWatermark publishes an idle watermark if the conditions are met for source.
func (iw *SrcIdleHandler) PublishSrcIdleWatermark(partitions []int32) bool {
	// if source is not idling, return
	if !iw.isSourceIdling() {
		return false
	}

	// if the step interval has not passed, return
	// if the last idle watermark published time is -1, it means that the idle watermark has not been published yet.
	// -1 is used as the default value for lastIdleWmPublishedTime, so that we immediately publish the idle watermark
	// when the source is idling for the first time after the threshold has passed and next subsequent idling happens
	// after the step interval has passed.
	if iw.lastIdleWmPublishedTime != time.UnixMilli(-1) && !iw.hasStepIntervalPassed() {
		return false
	}

	// publish the idle watermark, the idle watermark is the current watermark + the increment by value.
	nextIdleWM := iw.wmFetcher.ComputeWatermark().Add(iw.config.IdleSource.GetIncrementBy())
	currentTime := time.Now()

	// if the next idle watermark is after the current time, then set the next idle watermark to the current time.
	if nextIdleWM.After(currentTime) {
		nextIdleWM = currentTime
	}

	iw.srcPublisher.PublishIdleWatermarks(nextIdleWM, partitions)
	iw.lastIdleWmPublishedTime = time.Now()
	return true
}

// Reset resets the updatedTS to the current time.
func (iw *SrcIdleHandler) Reset() {
	iw.updatedTS = time.Now()
	iw.lastIdleWmPublishedTime = time.UnixMilli(-1)
}
