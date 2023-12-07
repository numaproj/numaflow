package idlehandler

import (
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
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
			iw := &SourceIdleHandler{
				config:                  tt.fields.config,
				lastIdleWmPublishedTime: tt.fields.lastIdleWmPublishedTime,
				updatedTS:               tt.fields.updatedTS,
			}
			time.Sleep(tt.sleep)
			if got := iw.IsSourceIdling(); got != tt.want {
				t.Errorf("IsSourceIdling() = %v, want %v", got, tt.want)
			}
		})
	}
}
