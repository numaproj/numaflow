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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum="";Pending;Running;Failed;Deleting
type ISBSvcPhase string

const (
	ISBSvcPhaseUnknown  ISBSvcPhase = ""
	ISBSvcPhasePending  ISBSvcPhase = "Pending"
	ISBSvcPhaseRunning  ISBSvcPhase = "Running"
	ISBSvcPhaseFailed   ISBSvcPhase = "Failed"
	ISBSvcPhaseDeleting ISBSvcPhase = "Deleting"

	// ISBSvcConditionConfigured has the status True when the InterStepBufferService
	// has valid configuration.
	ISBSvcConditionConfigured ConditionType = "Configured"
	// ISBSvcConditionDeployed has the status True when the InterStepBufferService
	// has its RestfulSet/Deployment as well as services created.
	ISBSvcConditionDeployed ConditionType = "Deployed"

	// ISBSvcConditionChildrenResourcesHealthy has the status True when the child resources are healthy.
	ISBSvcConditionChildrenResourcesHealthy ConditionType = "ChildrenResourcesHealthy"
)

type ISBSvcType string

const (
	ISBSvcTypeUnknown   ISBSvcType = ""
	ISBSvcTypeRedis     ISBSvcType = "redis"
	ISBSvcTypeJetStream ISBSvcType = "jetstream"
)

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=isbsvc
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.status.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
type InterStepBufferService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec InterStepBufferServiceSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	// +optional
	Status InterStepBufferServiceStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

func (isbs InterStepBufferService) GetType() ISBSvcType {
	if isbs.Spec.Redis != nil {
		return ISBSvcTypeRedis
	} else if isbs.Spec.JetStream != nil {
		return ISBSvcTypeJetStream
	}
	return ISBSvcTypeUnknown
}

// InterStepBufferServiceList is the list of InterStepBufferService resources
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InterStepBufferServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata" protobuf:"bytes,1,opt,name=metadata"`

	Items []InterStepBufferService `json:"items" protobuf:"bytes,2,rep,name=items"`
}

type InterStepBufferServiceSpec struct {
	Redis     *RedisBufferService     `json:"redis,omitempty" protobuf:"bytes,1,opt,name=redis"`
	JetStream *JetStreamBufferService `json:"jetstream,omitempty" protobuf:"bytes,2,opt,name=jetstream"`
}

type BufferServiceConfig struct {
	Redis     *RedisConfig     `json:"redis,omitempty" protobuf:"bytes,1,opt,name=redis"`
	JetStream *JetStreamConfig `json:"jetstream,omitempty" protobuf:"bytes,2,opt,name=jetstream"`
}

type InterStepBufferServiceStatus struct {
	Status             `json:",inline" protobuf:"bytes,1,opt,name=status"`
	Phase              ISBSvcPhase         `json:"phase,omitempty" protobuf:"bytes,2,opt,name=phase,casttype=ISBSvcPhase"`
	Message            string              `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	Config             BufferServiceConfig `json:"config,omitempty" protobuf:"bytes,4,opt,name=config"`
	Type               ISBSvcType          `json:"type,omitempty" protobuf:"bytes,5,opt,name=type"`
	ObservedGeneration int64               `json:"observedGeneration,omitempty" protobuf:"varint,6,opt,name=observedGeneration"`
}

func (iss *InterStepBufferServiceStatus) SetPhase(phase ISBSvcPhase, msg string) {
	iss.Phase = phase
	iss.Message = msg
}

func (iss *InterStepBufferServiceStatus) SetType(typ ISBSvcType) {
	iss.Type = typ
}

// InitConditions sets conditions to Unknown state.
func (iss *InterStepBufferServiceStatus) InitConditions() {
	iss.InitializeConditions(ISBSvcConditionConfigured, ISBSvcConditionDeployed, ISBSvcConditionChildrenResourcesHealthy)
	iss.SetPhase(ISBSvcPhasePending, "")
}

// MarkConfigured set the InterStepBufferService has valid configuration.
func (iss *InterStepBufferServiceStatus) MarkConfigured() {
	iss.MarkTrue(ISBSvcConditionConfigured)
	iss.SetPhase(ISBSvcPhasePending, "")
}

// MarkNotConfigured the InterStepBufferService has configuration.
func (iss *InterStepBufferServiceStatus) MarkNotConfigured(reason, message string) {
	iss.MarkFalse(ISBSvcConditionConfigured, reason, message)
	iss.SetPhase(ISBSvcPhaseFailed, message)
}

// MarkDeployed set the InterStepBufferService has been deployed.
func (iss *InterStepBufferServiceStatus) MarkDeployed() {
	iss.MarkTrue(ISBSvcConditionDeployed)
	iss.SetPhase(ISBSvcPhaseRunning, "")
}

// MarkDeployFailed set the InterStepBufferService deployment failed
func (iss *InterStepBufferServiceStatus) MarkDeployFailed(reason, message string) {
	iss.MarkFalse(ISBSvcConditionDeployed, reason, message)
	iss.SetPhase(ISBSvcPhaseFailed, message)
}

// SetObservedGeneration sets the Status ObservedGeneration
func (iss *InterStepBufferServiceStatus) SetObservedGeneration(value int64) {
	iss.ObservedGeneration = value
}

// IsHealthy indicates whether the InterStepBufferService is healthy or not
func (iss *InterStepBufferServiceStatus) IsHealthy() bool {
	// Deleting is a special case, we don't want to mark it as unhealthy as Pipeline reconciliation relies on it
	if iss.Phase != ISBSvcPhaseRunning && iss.Phase != ISBSvcPhaseDeleting {
		return false
	}
	return iss.IsReady()
}

// MarkChildrenResourceUnHealthy marks the children resources as not healthy
func (iss *InterStepBufferServiceStatus) MarkChildrenResourceUnHealthy(reason, message string) {
	iss.MarkFalse(ISBSvcConditionChildrenResourcesHealthy, reason, message)
	iss.Message = reason + ": " + message
}

// MarkChildrenResourceHealthy marks the children resources as healthy
func (iss *InterStepBufferServiceStatus) MarkChildrenResourceHealthy(reason, message string) {
	iss.MarkTrueWithReason(ISBSvcConditionChildrenResourcesHealthy, reason, message)
}
