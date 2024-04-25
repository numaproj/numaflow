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

package idlehandler

import (
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// Source Idle Handler has to resolve the following conundrums:
// How to decide if the source is idling?
//   If data forwarder is not reading any messages for WatermarkConfig.Threshold (provided by user)
//   time, then it is idling.
// When to publish the idle watermark?
//   If the source is idling and the step interval has passed (also provided by the user).
// What to publish as the idle watermark?
//   The current watermark + WatermarkConfig.IncrementBy (provided by user). We will ensure that the
//   increment will never cross (time.Now() - maxDelay).

// SourceIdleHandler handles operations related to idle watermarks for source.
type SourceIdleHandler struct {
	config                  *dfv1.Watermark
	lastPublishedIdleWm     time.Time
	lastIdleWmPublishedTime time.Time
	updatedTS               time.Time
	wmFetcher               fetch.SourceFetcher
	srcPublisher            publish.SourcePublisher
}

// NewSourceIdleHandler creates a new instance of SrcIdleHandler.
func NewSourceIdleHandler(config *dfv1.Watermark, fetcher fetch.SourceFetcher, publisher publish.SourcePublisher) *SourceIdleHandler {
	return &SourceIdleHandler{
		config:                  config,
		wmFetcher:               fetcher,
		srcPublisher:            publisher,
		updatedTS:               time.Now(),
		lastIdleWmPublishedTime: time.UnixMilli(-1),
		lastPublishedIdleWm:     time.UnixMilli(-1),
	}
}

// IsSourceIdling will return true if source has been idling and the step interval has passed.
func (iw *SourceIdleHandler) IsSourceIdling() bool {
	return iw.isSourceIdling() && iw.hasStepIntervalPassed()
}

// isSourceIdling checks if the source is idling by comparing the last updated timestamp with the threshold.
func (iw *SourceIdleHandler) isSourceIdling() bool {
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
func (iw *SourceIdleHandler) hasStepIntervalPassed() bool {

	// if the last idle watermark published time is -1, it means that the idle watermark has not been published yet.
	// -1 is used as the default value for lastIdleWmPublishedTime, so that we immediately publish the idle watermark
	// when the source is idling for the first time after the threshold has passed and next subsequent idle watermark
	// is published after the step interval has passed.
	if iw.lastIdleWmPublishedTime == time.UnixMilli(-1) {
		return true
	}

	// else make sure duration has passed
	return time.Since(iw.lastIdleWmPublishedTime) >= iw.config.IdleSource.GetStepInterval()
}

// PublishSourceIdleWatermark publishes an idle watermark.
func (iw *SourceIdleHandler) PublishSourceIdleWatermark(partitions []int32) {
	var nextIdleWM time.Time

	// compute the next idle watermark
	computedWm := iw.wmFetcher.ComputeWatermark()

	// check if the computed watermark is -1
	// last computed watermark can be -1, when the pod is restarted or when the processor entity is not created yet.
	if computedWm.UnixMilli() == -1 {
		// if the computed watermark is -1, it means that the source is not able to compute the watermark.
		// in this case, we can publish the idle watermark as the last published idle watermark + the increment by value.
		nextIdleWM = iw.lastPublishedIdleWm.Add(iw.config.IdleSource.GetIncrementBy())
	} else {
		// if its not -1, then we can publish the idle watermark as the computed watermark + the increment by value.
		nextIdleWM = computedWm.Add(iw.config.IdleSource.GetIncrementBy())
	}

	// if the next idle watermark is after the current time, then set the next idle watermark to the current time.
	currentTime := time.Now().Add(-1 * iw.config.GetMaxDelay())
	if nextIdleWM.After(currentTime) {
		nextIdleWM = currentTime
	}

	iw.srcPublisher.PublishIdleWatermarks(nextIdleWM, partitions)

	// update the last published idle watermark
	iw.lastPublishedIdleWm = nextIdleWM

	// set the last idle watermark published time to the current time
	iw.lastIdleWmPublishedTime = time.Now()
}

// Reset resets the updatedTS to the current time.
func (iw *SourceIdleHandler) Reset() {
	iw.updatedTS = time.Now()
	iw.lastIdleWmPublishedTime = time.UnixMilli(-1)
}
