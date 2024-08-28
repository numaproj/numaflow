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
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestApplyToPodSpec(t *testing.T) {
	runAsUser := int64(1001)
	runAsUser0 := int64(0)
	priority := int32(100)

	abstractPodTemplate := &AbstractPodTemplate{
		Metadata: &Metadata{
			Annotations: map[string]string{"my-annotation-name": "my-annotation-value"},
			Labels:      map[string]string{"my-label-name": "my-label-value"},
		},
		NodeSelector: map[string]string{"my-node-selector-name": "my-node-selector-value"},
		SecurityContext: &corev1.PodSecurityContext{
			RunAsUser:  &runAsUser,
			RunAsGroup: &runAsUser0,
		},
		Tolerations: []corev1.Toleration{
			{
				Key:      "my-toleration-key",
				Operator: "Equal",
				Value:    "my-toleration-value",
				Effect:   "NoSchedule",
			},
		},
		ImagePullSecrets: []corev1.LocalObjectReference{
			{
				Name: "template-image-pull-secret",
			},
		},
		PriorityClassName: "my-priority-class-name",
		Priority:          &priority,
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "m",
									Operator: "n",
									Values:   []string{"o"},
								},
							},
						},
					},
				},
			},
		},
		ServiceAccountName: "template-sa",
		ResourceClaims: []corev1.PodResourceClaim{
			{
				Name: "template-resource-claim",
			},
		},
	}

	podSpec := &corev1.PodSpec{
		ServiceAccountName: "spec-sa",
		ImagePullSecrets: []corev1.LocalObjectReference{
			{
				Name: "spec-image-pull-secret",
			},
		},
	}

	abstractPodTemplate.ApplyToPodSpec(podSpec)

	assert.Equal(t, podSpec.NodeSelector, abstractPodTemplate.NodeSelector)
	assert.Equal(t, podSpec.Tolerations, abstractPodTemplate.Tolerations)
	assert.Equal(t, podSpec.SecurityContext, abstractPodTemplate.SecurityContext)
	assert.Equal(t, podSpec.ImagePullSecrets[0].Name, "spec-image-pull-secret")
	assert.Equal(t, podSpec.PriorityClassName, abstractPodTemplate.PriorityClassName)
	assert.Equal(t, podSpec.Priority, abstractPodTemplate.Priority)
	assert.Equal(t, podSpec.Affinity, abstractPodTemplate.Affinity)
	assert.Equal(t, podSpec.ServiceAccountName, "spec-sa")
	assert.Equal(t, podSpec.ResourceClaims[0].Name, "template-resource-claim")
}
