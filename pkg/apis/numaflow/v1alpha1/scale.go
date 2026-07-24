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

package v1alpha1

import (
	"time"

	cron "github.com/robfig/cron/v3"
)

// CronScheduling defines cron-based autoscaling overrides.
type CronScheduling struct {
	// Timezone for interpreting cron expressions. IANA Time Zone Database format.
	Timezone  string         `json:"timezone" protobuf:"bytes,1,opt,name=timezone"`
	Schedules []CronSchedule `json:"schedules" protobuf:"bytes,2,rep,name=schedules"`
}

type CronSchedule struct {
	// Start of the cron window. Linux cron format (Minute Hour Dom Month Dow).
	Start string `json:"start" protobuf:"bytes,1,opt,name=start"`
	// End of the cron window. Same format as Start. Must differ from Start.
	End string `json:"end" protobuf:"bytes,2,opt,name=end"`
	// Minimum replicas during this window. Overrides scale.min.
	Min *int32 `json:"min" protobuf:"varint,3,opt,name=min"`
	// Maximum replicas during this window. Overrides scale.max.
	Max *int32 `json:"max" protobuf:"varint,4,opt,name=max"`
}

// IsActiveAt reports whether time t falls within this cron window.
func (cs CronSchedule) IsActiveAt(t time.Time) bool {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	startSched, err := parser.Parse(cs.Start)
	if err != nil {
		return false
	}
	endSched, err := parser.Parse(cs.End)
	if err != nil {
		return false
	}
	nextStart := startSched.Next(t)
	nextEnd := endSched.Next(t)
	return nextEnd.Before(nextStart)
}

// Scale defines the parameters for autoscaling.
type Scale struct {
	// Whether to disable autoscaling.
	// Set to "true" when using Kubernetes HPA or any other 3rd party autoscaling strategies.
	// +optional
	Disabled bool `json:"disabled,omitempty" protobuf:"bytes,1,opt,name=disabled"`
	// Minimum replicas.
	// +optional
	Min *int32 `json:"min,omitempty" protobuf:"varint,2,opt,name=min"`
	// Maximum replicas.
	// +optional
	Max *int32 `json:"max,omitempty" protobuf:"varint,3,opt,name=max"`
	// Lookback seconds to calculate the average pending messages and processing rate.
	// +optional
	LookbackSeconds *uint32 `json:"lookbackSeconds,omitempty" protobuf:"varint,4,opt,name=lookbackSeconds"`
	// After scaling down the source vertex to 0, sleep how many seconds before scaling the source vertex back up to peek.
	// +optional
	ZeroReplicaSleepSeconds *uint32 `json:"zeroReplicaSleepSeconds,omitempty" protobuf:"varint,5,opt,name=zeroReplicaSleepSeconds"`
	// TargetProcessingSeconds is used to tune the aggressiveness of autoscaling for source vertices, it measures how fast
	// you want the vertex to process all the pending messages. Typically increasing the value, which leads to lower processing
	// rate, thus less replicas. It's only effective for source vertices.
	// +optional
	TargetProcessingSeconds *uint32 `json:"targetProcessingSeconds,omitempty" protobuf:"varint,6,opt,name=targetProcessingSeconds"`
	// TargetBufferAvailability is used to define the target percentage of the buffer availability.
	// A valid and meaningful value should be less than the BufferUsageLimit defined in the Edge spec (or Pipeline spec), for example, 50.
	// It only applies to UDF and Sink vertices because only they have buffers to read.
	// +optional
	TargetBufferAvailability *uint32 `json:"targetBufferAvailability,omitempty" protobuf:"varint,7,opt,name=targetBufferAvailability"`
	// DeprecatedReplicasPerScale defines the number of maximum replicas that can be changed in a single scale up or down operation.
	// The is use to prevent from too aggressive scaling operations
	// Deprecated: Use ReplicasPerScaleUp and ReplicasPerScaleDown instead
	// +optional
	DeprecatedReplicasPerScale *uint32 `json:"replicasPerScale,omitempty" protobuf:"varint,8,opt,name=replicasPerScale"`
	// ScaleUpCooldownSeconds defines the cooldown seconds after a scaling operation, before a follow-up scaling up.
	// It defaults to the CooldownSeconds if not set.
	// +optional
	ScaleUpCooldownSeconds *uint32 `json:"scaleUpCooldownSeconds,omitempty" protobuf:"varint,9,opt,name=scaleUpCooldownSeconds"`
	// ScaleDownCooldownSeconds defines the cooldown seconds after a scaling operation, before a follow-up scaling down.
	// It defaults to the CooldownSeconds if not set.
	// +optional
	ScaleDownCooldownSeconds *uint32 `json:"scaleDownCooldownSeconds,omitempty" protobuf:"varint,10,opt,name=scaleDownCooldownSeconds"`
	// ReplicasPerScaleUp defines the number of maximum replicas that can be changed in a single scaled up operation.
	// The is use to prevent from too aggressive scaling up operations
	// +optional
	ReplicasPerScaleUp *uint32 `json:"replicasPerScaleUp,omitempty" protobuf:"varint,11,opt,name=replicasPerScaleUp"`
	// ReplicasPerScaleDown defines the number of maximum replicas that can be changed in a single scaled down operation.
	// The is use to prevent from too aggressive scaling down operations
	// +optional
	ReplicasPerScaleDown *uint32 `json:"replicasPerScaleDown,omitempty" protobuf:"varint,12,opt,name=replicasPerScaleDown"`
	// Cron defines time-based autoscaling overrides. During active cron windows, the window's
	// min/max replace the base min/max for scaling decisions.
	// +optional
	Cron *CronScheduling `json:"cron,omitempty" protobuf:"bytes,13,opt,name=cron"`
}

// GetActiveCronSchedule returns the active cron schedule at the given time.
func (s Scale) GetActiveCronSchedule(at time.Time) (*CronSchedule, bool) {
	if s.Cron == nil || len(s.Cron.Schedules) == 0 {
		return nil, false
	}
	loc, err := time.LoadLocation(s.Cron.Timezone)
	if err != nil {
		return nil, false
	}
	at = at.In(loc)
	for i := range s.Cron.Schedules {
		if s.Cron.Schedules[i].IsActiveAt(at) {
			return &s.Cron.Schedules[i], true
		}
	}
	return nil, false
}

// GetEffectiveScaleBounds returns the min/max replicas and whether a cron schedule is active.
func (s Scale) GetEffectiveScaleBounds() (int32, int32, bool) {
	minR := s.GetMinReplicas()
	maxR := s.GetMaxReplicas()
	sched, active := s.GetActiveCronSchedule(time.Now())
	if !active {
		return minR, maxR, false
	}
	if sched.Min != nil {
		minR = *sched.Min
	}
	if sched.Max != nil {
		maxR = *sched.Max
	}
	return minR, maxR, true
}

func (s Scale) GetLookbackSeconds() int {
	if s.LookbackSeconds != nil {
		// do not allow the value to be larger than the MaxLookbackSeconds in our config
		return min(MaxLookbackSeconds, int(*s.LookbackSeconds))
	}
	return DefaultLookbackSeconds
}

func (s Scale) GetScaleUpCooldownSeconds() int {
	if s.ScaleUpCooldownSeconds != nil {
		return int(*s.ScaleUpCooldownSeconds)
	}
	return DefaultCooldownSeconds
}

func (s Scale) GetScaleDownCooldownSeconds() int {
	if s.ScaleDownCooldownSeconds != nil {
		return int(*s.ScaleDownCooldownSeconds)
	}
	return DefaultCooldownSeconds
}

func (s Scale) GetZeroReplicaSleepSeconds() int {
	if s.ZeroReplicaSleepSeconds != nil {
		return int(*s.ZeroReplicaSleepSeconds)
	}
	return DefaultZeroReplicaSleepSeconds
}

func (s Scale) GetTargetProcessingSeconds() int {
	if s.TargetProcessingSeconds != nil {
		return int(*s.TargetProcessingSeconds)
	}
	return DefaultTargetProcessingSeconds
}

func (s Scale) GetTargetBufferAvailability() int {
	if s.TargetBufferAvailability != nil {
		return int(*s.TargetBufferAvailability)
	}
	return DefaultTargetBufferAvailability
}

func (s Scale) GetReplicasPerScaleUp() int {
	if s.ReplicasPerScaleUp != nil {
		return int(*s.ReplicasPerScaleUp)
	}
	if s.DeprecatedReplicasPerScale != nil {
		return int(*s.DeprecatedReplicasPerScale)
	}
	return DefaultReplicasPerScale
}

func (s Scale) GetReplicasPerScaleDown() int {
	if s.ReplicasPerScaleDown != nil {
		return int(*s.ReplicasPerScaleDown)
	}
	if s.DeprecatedReplicasPerScale != nil {
		return int(*s.DeprecatedReplicasPerScale)
	}
	return DefaultReplicasPerScale
}

func (s Scale) GetMinReplicas() int32 {
	if x := s.Min; x == nil || *x < 0 {
		return 0
	} else {
		return *x
	}
}

func (s Scale) GetMaxReplicas() int32 {
	if x := s.Max; x == nil {
		return DefaultMaxReplicas
	} else {
		return *x
	}
}
