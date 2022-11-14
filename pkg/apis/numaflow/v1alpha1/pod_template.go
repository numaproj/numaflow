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

// AbstractPodTemplate provides a template for pod customization in vertices, daemon deployments and so on.
type AbstractPodTemplate struct {
	// Metadata sets the pods's metadata, i.e. annotations and labels
	// +optional
	Metadata *Metadata `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// NodeSelector is a selector which must be true for the pod to fit on a node.
	// Selector which must match a node's labels for the pod to be scheduled on that node.
	// More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty" protobuf:"bytes,2,rep,name=nodeSelector"`
	// If specified, the pod's tolerations.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty" protobuf:"bytes,3,rep,name=tolerations"`
	// SecurityContext holds pod-level security attributes and common container settings.
	// Optional: Defaults to empty.  See type description for default values of each field.
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty" protobuf:"bytes,4,opt,name=securityContext"`
	// ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec.
	// If specified, these secrets will be passed to individual puller implementations for them to use. For example,
	// in the case of docker, only DockerConfig type secrets are honored.
	// More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,5,rep,name=imagePullSecrets"`
	// If specified, indicates the Redis pod's priority. "system-node-critical"
	// and "system-cluster-critical" are two special keywords which indicate the
	// highest priorities with the former being the highest priority. Any other
	// name must be defined by creating a PriorityClass object with that name.
	// If not specified, the pod priority will be default or zero if there is no
	// default.
	// More info: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,6,opt,name=priorityClassName"`
	// The priority value. Various system components use this field to find the
	// priority of the Redis pod. When Priority Admission Controller is enabled,
	// it prevents users from setting this field. The admission controller populates
	// this field from PriorityClassName.
	// The higher the value, the higher the priority.
	// More info: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/
	// +optional
	Priority *int32 `json:"priority,omitempty" protobuf:"bytes,7,opt,name=priority"`
	// The pod's scheduling constraints
	// More info: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty" protobuf:"bytes,8,opt,name=affinity"`
	// ServiceAccountName applied to the pod
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty" protobuf:"bytes,9,opt,name=serviceAccountName"`
}

// ApplyToPodSpec updates the PodSpec with the values in the AbstractPodTemplate
func (apt *AbstractPodTemplate) ApplyToPodSpec(ps *corev1.PodSpec) {
	ps.NodeSelector = apt.NodeSelector
	ps.Tolerations = apt.Tolerations
	ps.SecurityContext = apt.SecurityContext
	ps.ImagePullSecrets = apt.ImagePullSecrets
	ps.PriorityClassName = apt.PriorityClassName
	ps.Priority = apt.Priority
	ps.Affinity = apt.Affinity
	ps.ServiceAccountName = apt.ServiceAccountName
}

// ApplyToPodTemplateSpec updates the PodTemplateSpec with the values in the AbstractPodTemplate
// Labels and Annotations will be appended, individual labels or annotations in original PodTemplateSpec will not be overridden
func (apt *AbstractPodTemplate) ApplyToPodTemplateSpec(p *corev1.PodTemplateSpec) {
	apt.ApplyToPodSpec(&p.Spec)
	if apt.Metadata != nil && len(apt.Metadata.Labels) > 0 {
		if p.Labels == nil {
			p.Labels = map[string]string{}
		}
		for k, v := range apt.Metadata.Labels {
			if _, ok := p.Labels[k]; !ok {
				p.Labels[k] = v
			}
		}
	}
	if apt.Metadata != nil && len(apt.Metadata.Annotations) > 0 {
		if p.Annotations == nil {
			p.Annotations = map[string]string{}
		}
		for k, v := range apt.Metadata.Annotations {
			if _, ok := p.Annotations[k]; !ok {
				p.Annotations[k] = v
			}
		}
	}
}
