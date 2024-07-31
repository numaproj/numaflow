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
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSourceIdleHandler_IsSourceIdling(t *testing.T) {
	type fields struct {
		config                  *dfv1.Watermark
		lastIdleWmPublishedTime time.Time
		updatedTS               time.Time
	}
	tests := []struct {
		name   string
		fields fields
		sleep  time.Duration
		want   bool
	}{
		{
			name: "Source is not idling as threshold has not passed",
			fields: fields{
				config: &dfv1.Watermark{
					IdleSource: &dfv1.IdleSource{
						Threshold:    &metav1.Duration{Duration: 5 * time.Second},
						StepInterval: &metav1.Duration{Duration: 2 * time.Second},
						IncrementBy:  &metav1.Duration{Duration: 3 * time.Second},
					},
				},
				lastIdleWmPublishedTime: time.UnixMilli(-1),
				updatedTS:               time.Now(),
			},
			sleep: 0 * time.Second,
			want:  false,
		},
		{
			name: "Source is idling as threshold has passed",
			fields: fields{
				config: &dfv1.Watermark{
					IdleSource: &dfv1.IdleSource{
						Threshold:    &metav1.Duration{Duration: 5 * time.Second},
						StepInterval: &metav1.Duration{Duration: 2 * time.Second},
						IncrementBy:  &metav1.Duration{Duration: 3 * time.Second},
					},
				},
				lastIdleWmPublishedTime: time.UnixMilli(-1),
				updatedTS:               time.Now(),
			},
			sleep: 5 * time.Second,
			want:  true,
		},
		{
			name: "Source is idling, threshold and step interval has passed",
			fields: fields{
				config: &dfv1.Watermark{
					IdleSource: &dfv1.IdleSource{
						Threshold:    &metav1.Duration{Duration: 5 * time.Second},
						StepInterval: &metav1.Duration{Duration: 2 * time.Second},
						IncrementBy:  &metav1.Duration{Duration: 3 * time.Second},
					},
				},
				lastIdleWmPublishedTime: time.Now(),
				updatedTS:               time.Now(),
			},
			sleep: 5 * time.Second,
			want:  true,
		},
		{
			name: "Watermark is not enabled for source",
			fields: fields{
				config: &dfv1.Watermark{
					IdleSource: nil,
				},
				lastIdleWmPublishedTime: time.UnixMilli(-1),
				updatedTS:               time.Now(),
			},
			sleep: 0 * time.Second,
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iw := NewSourceIdleHandler(tt.fields.config, nil, nil)
			iw.lastIdleWmPublishedTime = tt.fields.lastIdleWmPublishedTime
			time.Sleep(tt.sleep)
			if got := iw.IsSourceIdling(); got != tt.want {
				t.Errorf("IsSourceIdling() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Mock SourceFetcher and SourcePublisher for testing
type MockSourceFetcher struct {
	mock.Mock
}

func (m *MockSourceFetcher) ComputeWatermark() wmb.Watermark {
	args := m.Called()
	return args.Get(0).(wmb.Watermark)
}

func (m *MockSourceFetcher) ComputeHeadWatermark(fromPartitionIdx int32) wmb.Watermark {
	args := m.Called(fromPartitionIdx)
	return args.Get(0).(wmb.Watermark)
}

type MockSourcePublisher struct {
	mock.Mock
}

func (m *MockSourcePublisher) PublishIdleWatermarks(watermark time.Time, partitions []int32) {
	m.Called(watermark, partitions)
}

func (m *MockSourcePublisher) PublishSourceWatermarks(in []*isb.ReadMessage) {
	m.Called(in)
}

func TestSourceIdleHandler_Reset(t *testing.T) {
	config := &dfv1.Watermark{}
	mockFetcher := new(MockSourceFetcher)
	mockPublisher := new(MockSourcePublisher)

	handler := NewSourceIdleHandler(config, mockFetcher, mockPublisher)
	handler.lastIdleWmPublishedTime = time.Now()

	handler.Reset()

	assert.WithinDuration(t, time.Now(), handler.updatedTS, time.Second)
	assert.Equal(t, time.UnixMilli(-1), handler.lastIdleWmPublishedTime)
}
