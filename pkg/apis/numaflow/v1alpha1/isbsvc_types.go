/*


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

// +kubebuilder:validation:Enum="";Pending;Running;Failed
type ISBSvcPhase string

const (
	ISBSvcPhaseUnknown ISBSvcPhase = ""
	ISBSvcPhasePending ISBSvcPhase = "Pending"
	ISBSvcPhaseRunning ISBSvcPhase = "Running"
	ISBSvcPhaseFailed  ISBSvcPhase = "Failed"

	// ISBSvcConditionConfigured has the status True when the InterStepBufferService
	// has valid configuration.
	ISBSvcConditionConfigured ConditionType = "Configured"
	// ISBSvcConditionDeployed has the status True when the InterStepBufferService
	// has its RestfulSet/Deployment as well as services created.
	ISBSvcConditionDeployed ConditionType = "Deployed"
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
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
type InterStepBufferService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec InterStepBufferServiceSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	// +optional
	Status InterStepBufferServiceStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
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
	Status  `json:",inline" protobuf:"bytes,1,opt,name=status"`
	Phase   ISBSvcPhase         `json:"phase,omitempty" protobuf:"bytes,2,opt,name=phase,casttype=ISBSvcPhase"`
	Message string              `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	Config  BufferServiceConfig `json:"config,omitempty" protobuf:"bytes,4,opt,name=config"`
	Type    ISBSvcType          `json:"type,omitempty" protobuf:"bytes,5,opt,name=type"`
}

func (isbsvc *InterStepBufferServiceStatus) SetPhase(phase ISBSvcPhase, msg string) {
	isbsvc.Phase = phase
	isbsvc.Message = msg
}

func (isbsvc *InterStepBufferServiceStatus) SetType(typ ISBSvcType) {
	isbsvc.Type = typ
}

// InitConditions sets conditions to Unknown state.
func (isbsvc *InterStepBufferServiceStatus) InitConditions() {
	isbsvc.InitializeConditions(ISBSvcConditionConfigured, ISBSvcConditionDeployed)
	isbsvc.SetPhase(ISBSvcPhasePending, "")
}

// MarkConfigured set the InterStepBufferService has valid configuration.
func (isbsvc *InterStepBufferServiceStatus) MarkConfigured() {
	isbsvc.MarkTrue(ISBSvcConditionConfigured)
	isbsvc.SetPhase(ISBSvcPhasePending, "")
}

// MarkNotConfigured the InterStepBufferService has configuration.
func (isbsvc *InterStepBufferServiceStatus) MarkNotConfigured(reason, message string) {
	isbsvc.MarkFalse(ISBSvcConditionConfigured, reason, message)
	isbsvc.SetPhase(ISBSvcPhaseFailed, message)
}

// MarkDeployed set the InterStepBufferService has been deployed.
func (isbsvc *InterStepBufferServiceStatus) MarkDeployed() {
	isbsvc.MarkTrue(ISBSvcConditionDeployed)
	isbsvc.SetPhase(ISBSvcPhaseRunning, "")
}

// MarkDeployFailed set the InterStepBufferService deployment failed
func (isbsvc *InterStepBufferServiceStatus) MarkDeployFailed(reason, message string) {
	isbsvc.MarkFalse(ISBSvcConditionDeployed, reason, message)
	isbsvc.SetPhase(ISBSvcPhaseFailed, message)
}
