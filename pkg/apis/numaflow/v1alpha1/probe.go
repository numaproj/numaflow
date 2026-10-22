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

import corev1 "k8s.io/api/core/v1"

// Probe is used to customize the configuration for Readiness and Liveness probes.
type Probe struct {
	// Number of seconds after the container has started before liveness probes are initiated.
	// More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
	// +optional
	InitialDelaySeconds *int32 `json:"initialDelaySeconds,omitempty" protobuf:"varint,1,opt,name=initialDelaySeconds"`
	// Number of seconds after which the probe times out.
	// More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty" protobuf:"varint,2,opt,name=timeoutSeconds"`
	// How often (in seconds) to perform the probe.
	// +optional
	PeriodSeconds *int32 `json:"periodSeconds,omitempty" protobuf:"varint,3,opt,name=periodSeconds"`
	// Minimum consecutive successes for the probe to be considered successful after having failed.
	// Defaults to 1. Must be 1 for liveness and startup. Minimum value is 1.
	// +optional
	SuccessThreshold *int32 `json:"successThreshold,omitempty" protobuf:"varint,4,opt,name=successThreshold"`
	// Minimum consecutive failures for the probe to be considered failed after having succeeded.
	// Defaults to 3. Minimum value is 1.
	// +optional
	FailureThreshold *int32 `json:"failureThreshold,omitempty" protobuf:"varint,5,opt,name=failureThreshold"`
}

func GetProbeInitialDelaySecondsOr(probe *Probe, defaultValue int32) int32 {
	if probe == nil || probe.InitialDelaySeconds == nil {
		return defaultValue
	}
	return *probe.InitialDelaySeconds
}

func GetProbeTimeoutSecondsOr(probe *Probe, defaultValue int32) int32 {
	if probe == nil || probe.TimeoutSeconds == nil {
		return defaultValue
	}
	return *probe.TimeoutSeconds
}

func GetProbePeriodSecondsOr(probe *Probe, defaultValue int32) int32 {
	if probe == nil || probe.PeriodSeconds == nil {
		return defaultValue
	}
	return *probe.PeriodSeconds
}

func GetProbeSuccessThresholdOr(probe *Probe, defaultValue int32) int32 {
	if probe == nil || probe.SuccessThreshold == nil {
		return defaultValue
	}
	return *probe.SuccessThreshold
}
func GetProbeFailureThresholdOr(probe *Probe, defaultValue int32) int32 {
	if probe == nil || probe.FailureThreshold == nil {
		return defaultValue
	}
	return *probe.FailureThreshold
}

// startupProbeFrom builds a startup probe running the same check as the given liveness probe, so
// that a slow first start is bounded by the startup probe rather than the liveness budget. Unset
// fields fall back to the liveness probe's, and nil is returned unless a startup probe was asked
// for and there is a liveness check to derive it from.
func startupProbeFrom(startup *Probe, liveness *corev1.Probe) *corev1.Probe {
	if startup == nil || liveness == nil {
		return nil
	}
	return &corev1.Probe{
		ProbeHandler:        liveness.ProbeHandler,
		InitialDelaySeconds: GetProbeInitialDelaySecondsOr(startup, liveness.InitialDelaySeconds),
		PeriodSeconds:       GetProbePeriodSecondsOr(startup, liveness.PeriodSeconds),
		TimeoutSeconds:      GetProbeTimeoutSecondsOr(startup, liveness.TimeoutSeconds),
		FailureThreshold:    GetProbeFailureThresholdOr(startup, liveness.FailureThreshold),
	}
}
