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
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

var (
	testPipeline = &Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testPipelineName,
		},
		Spec: PipelineSpec{
			Vertices: []AbstractVertex{
				{Name: "input", Source: &Source{}},
				{Name: "p1", UDF: &UDF{}},
				{Name: "output", Sink: &Sink{}},
			},
			Edges: []Edge{
				{From: "input", To: "p1"},
				{From: "p1", To: "output"},
			},
		},
	}
)

func Test_ListAllEdges(t *testing.T) {
	es := testPipeline.ListAllEdges()
	assert.Equal(t, 2, len(es))
	pl := testPipeline.DeepCopy()
	pl.Spec.Vertices[1].UDF.GroupBy = &GroupBy{}
	es = pl.ListAllEdges()
	assert.Equal(t, 2, len(es))
	es = pl.ListAllEdges()
	assert.Equal(t, 2, len(es))
}

func Test_GetToEdges(t *testing.T) {
	es := testPipeline.GetToEdges("p1")
	assert.Equal(t, 1, len(es))
	assert.Equal(t, "output", es[0].To)
	es = testPipeline.GetToEdges("output")
	assert.Equal(t, 0, len(es))
}

func Test_GetFromEdges(t *testing.T) {
	es := testPipeline.GetFromEdges("p1")
	assert.Equal(t, 1, len(es))
	assert.Equal(t, "input", es[0].From)
	es = testPipeline.GetFromEdges("output")
	assert.Equal(t, 1, len(es))
	assert.Equal(t, "p1", es[0].From)
	es = testPipeline.GetFromEdges("input")
	assert.Equal(t, 0, len(es))
}

func Test_GetAllBuffers(t *testing.T) {
	s := testPipeline.GetAllBuffers()
	assert.Equal(t, 2, len(s))
	assert.Contains(t, s, testPipeline.Namespace+"-"+testPipeline.Name+"-p1-0")
	assert.Contains(t, s, testPipeline.Namespace+"-"+testPipeline.Name+"-output-0")
}

func Test_GetVertex(t *testing.T) {
	v := testPipeline.GetVertex("abc")
	assert.Nil(t, v)
	v = testPipeline.GetVertex("input")
	assert.NotNil(t, v)
}

func TestGetDaemonServiceName(t *testing.T) {
	n := testPipeline.GetDaemonServiceName()
	assert.Equal(t, testPipeline.Name+"-daemon-svc", n)
}

func TestGetDaemonDeployName(t *testing.T) {
	n := testPipeline.GetDaemonDeploymentName()
	assert.Equal(t, testPipeline.Name+"-daemon", n)
}

func TestGetDaemonSvcObj(t *testing.T) {
	s := testPipeline.GetDaemonServiceObj()
	assert.Equal(t, s.Name, testPipeline.GetDaemonServiceName())
	assert.Equal(t, s.Namespace, testPipeline.Namespace)
	assert.Equal(t, 1, len(s.Spec.Ports))
	assert.Equal(t, DaemonServicePort, int(s.Spec.Ports[0].Port))
}

func TestGetDaemonDeploy(t *testing.T) {
	req := GetDaemonDeploymentReq{
		ISBSvcType: ISBSvcTypeRedis,
		Image:      testFlowImage,
		PullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			{Name: "test-env", Value: "test-val"},
		},
	}
	t.Run("test get deployment obj", func(t *testing.T) {
		s, err := testPipeline.GetDaemonDeploymentObj(req)
		assert.NoError(t, err)
		assert.Equal(t, testPipeline.GetDaemonDeploymentName(), s.Name)
		assert.Equal(t, 1, len(s.Spec.Template.Spec.Containers))
		assert.Equal(t, 1, len(s.Spec.Template.Spec.InitContainers))
		assert.Equal(t, req.Image, s.Spec.Template.Spec.Containers[0].Image)
		assert.Equal(t, req.Image, s.Spec.Template.Spec.InitContainers[0].Image)
		assert.Contains(t, s.Spec.Template.Spec.Containers[0].Args, "daemon-server")
		envNames := []string{}
		for _, e := range s.Spec.Template.Spec.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, "test-env")
	})

	t.Run("test liveness and readiness probe", func(t *testing.T) {
		s, err := testPipeline.GetDaemonDeploymentObj(req)
		assert.NoError(t, err)
		assert.NotNil(t, s.Spec.Template.Spec.Containers[0].LivenessProbe)
		assert.NotNil(t, s.Spec.Template.Spec.Containers[0].ReadinessProbe)
	})

	t.Run("test get init container", func(t *testing.T) {
		c := testPipeline.getDaemonPodInitContainer(req)
		assert.Equal(t, CtrInit, c.Name)
		assert.Equal(t, req.Image, c.Image)
		assert.Contains(t, c.Args, "isbsvc-validate")
	})

	t.Run("test get deployment obj with pipeline overrides", func(t *testing.T) {
		env := corev1.EnvVar{Name: "my-env-name", Value: "my-env-value"}
		initEnv := corev1.EnvVar{Name: "my-init-env-name", Value: "my-init-env-value"}
		podLabels := map[string]string{"my-label-name": "my-label-value"}
		podAnnotations := map[string]string{"my-annotation-name": "my-annotation-value"}
		replicas := int32(2)
		nodeSelector := map[string]string{"my-node-selector-name": "my-node-selector-value"}
		priority := int32(100)
		toleration := corev1.Toleration{
			Key:      "my-toleration-key",
			Operator: "Equal",
			Value:    "my-toleration-value",
			Effect:   "NoSchedule",
		}
		pl := testPipeline.DeepCopy()
		pl.Spec.Templates = &Templates{
			DaemonTemplate: &DaemonTemplate{
				ContainerTemplate: &ContainerTemplate{
					Resources: testResources,
					Env:       []corev1.EnvVar{env},
					SecurityContext: &corev1.SecurityContext{
						Privileged: pointer.Bool(false),
					},
				},
				InitContainerTemplate: &ContainerTemplate{
					Resources: testResources,
					Env:       []corev1.EnvVar{initEnv},
				},
				AbstractPodTemplate: AbstractPodTemplate{
					Metadata: &Metadata{
						Annotations: podAnnotations,
						Labels:      podLabels,
					},
					NodeSelector:      nodeSelector,
					Tolerations:       []corev1.Toleration{toleration},
					PriorityClassName: "my-priority-class-name",
					Priority:          &priority,
				},
				Replicas: &replicas,
			},
		}
		s, err := pl.GetDaemonDeploymentObj(req)
		assert.NoError(t, err)
		assert.Equal(t, pl.GetDaemonDeploymentName(), s.Name)
		assert.Equal(t, 1, len(s.Spec.Template.Spec.Containers))
		assert.Greater(t, len(s.Spec.Template.Spec.Containers[0].Env), 1)
		assert.Contains(t, s.Spec.Template.Spec.Containers[0].Env, env)
		assert.NotNil(t, s.Spec.Template.Spec.Containers[0].SecurityContext)
		assert.NotNil(t, s.Spec.Template.Spec.Containers[0].SecurityContext.Privileged)
		assert.False(t, *s.Spec.Template.Spec.Containers[0].SecurityContext.Privileged)
		assert.Equal(t, 1, len(s.Spec.Template.Spec.InitContainers))
		assert.Equal(t, s.Spec.Template.Spec.InitContainers[0].Resources, testResources)
		assert.Contains(t, s.Spec.Template.Spec.InitContainers[0].Env, initEnv)
		assert.Greater(t, len(s.Spec.Template.Labels), len(podLabels))
		assert.Equal(t, s.Spec.Template.Labels["my-label-name"], podLabels["my-label-name"])
		assert.Equal(t, s.Spec.Template.Annotations["my-annotation-name"], podAnnotations["my-annotation-name"])
		assert.NotNil(t, s.Spec.Replicas)
		assert.Equal(t, *s.Spec.Replicas, replicas)
		assert.Equal(t, s.Spec.Template.Spec.NodeSelector["my-node-selector-name"], nodeSelector["my-node-selector-name"])
		assert.NotNil(t, s.Spec.Template.Spec.Priority)
		assert.Equal(t, *s.Spec.Template.Spec.Priority, priority)
		assert.Contains(t, s.Spec.Template.Spec.Tolerations, toleration)
		assert.Equal(t, s.Spec.Template.Spec.PriorityClassName, "my-priority-class-name")
	})
}

func Test_PipelineVertexCounts(t *testing.T) {
	s := PipelineStatus{}
	s.SetVertexCounts(testPipeline.Spec.Vertices)
	assert.Equal(t, uint32(3), *s.VertexCount)
	assert.Equal(t, uint32(1), *s.SourceCount)
	assert.Equal(t, uint32(1), *s.SinkCount)
	assert.Equal(t, uint32(1), *s.UDFCount)
}

func Test_PipelineSetPhase(t *testing.T) {
	s := PipelineStatus{}
	s.SetPhase(PipelinePhaseRunning, "message")
	assert.Equal(t, "message", s.Message)
	assert.Equal(t, PipelinePhaseRunning, s.Phase)
}

func Test_PipelineInitConditions(t *testing.T) {
	s := PipelineStatus{}
	s.InitConditions()
	assert.Equal(t, 2, len(s.Conditions))
	for _, c := range s.Conditions {
		assert.Equal(t, metav1.ConditionUnknown, c.Status)
	}
}

func Test_PipelineMarkStatus(t *testing.T) {
	s := PipelineStatus{}
	s.InitConditions()
	s.MarkNotConfigured("reason", "message")
	for _, c := range s.Conditions {
		if c.Type == string(PipelineConditionConfigured) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
		}
	}
	s.MarkConfigured()
	for _, c := range s.Conditions {
		if c.Type == string(PipelineConditionConfigured) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
		}
	}
	s.MarkDeployFailed("reason", "message")
	for _, c := range s.Conditions {
		if c.Type == string(PipelineConditionDeployed) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
		}
	}
	s.MarkDeployed()
	for _, c := range s.Conditions {
		if c.Type == string(PipelineConditionDeployed) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
		}
	}
	assert.True(t, s.IsReady())
}

func Test_PipelineMarkPhases(t *testing.T) {
	s := PipelineStatus{}
	s.MarkPhaseDeleting()
	assert.Equal(t, PipelinePhaseDeleting, s.Phase)
	s.MarkPhasePaused()
	assert.Equal(t, PipelinePhasePaused, s.Phase)
	s.MarkPhasePausing()
	assert.Equal(t, PipelinePhasePausing, s.Phase)
	s.MarkPhaseRunning()
	assert.Equal(t, PipelinePhaseRunning, s.Phase)
}

func Test_GetDownstreamEdges(t *testing.T) {
	pl := Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pl",
			Namespace: "test-ns",
		},
		Spec: PipelineSpec{
			Vertices: []AbstractVertex{
				{Name: "input"},
				{Name: "p1"},
				{Name: "p2"},
				{Name: "p11"},
				{Name: "output"},
			},
			Edges: []Edge{
				{From: "input", To: "p1"},
				{From: "p1", To: "p11"},
				{From: "p1", To: "p1"},
				{From: "p1", To: "p2"},
				{From: "p2", To: "output"},
			},
		},
	}
	edges := pl.GetDownstreamEdges("input")
	assert.Equal(t, 5, len(edges))
	assert.Equal(t, edges, pl.ListAllEdges())
	assert.Equal(t, edges[2], Edge{From: "p1", To: "p1"})
	assert.Equal(t, edges[3], Edge{From: "p1", To: "p2"})

	edges = pl.GetDownstreamEdges("p1")
	assert.Equal(t, 4, len(edges))

	edges = pl.GetDownstreamEdges("p2")
	assert.Equal(t, 1, len(edges))

	edges = pl.GetDownstreamEdges("p11")
	assert.Equal(t, 0, len(edges))

	edges = pl.GetDownstreamEdges("output")
	assert.Equal(t, 0, len(edges))

	edges = pl.GetDownstreamEdges("notexisting")
	assert.Equal(t, 0, len(edges))
}

func Test_GetWatermarkMaxDelay(t *testing.T) {
	wm := Watermark{}
	assert.Equal(t, "0s", wm.GetMaxDelay().String())
	wm.MaxDelay = &metav1.Duration{Duration: time.Duration(2 * time.Second)}
	assert.Equal(t, "2s", wm.GetMaxDelay().String())
}

func Test_GetDeleteGracePeriodSeconds(t *testing.T) {
	lc := Lifecycle{}
	assert.Equal(t, int32(30), lc.GetDeleteGracePeriodSeconds())
	lc.DeleteGracePeriodSeconds = pointer.Int32(50)
	assert.Equal(t, int32(50), lc.GetDeleteGracePeriodSeconds())
}

func Test_GetDesiredPhase(t *testing.T) {
	lc := Lifecycle{}
	assert.Equal(t, PipelinePhaseRunning, lc.GetDesiredPhase())
	lc.DesiredPhase = PipelinePhasePaused
	assert.Equal(t, PipelinePhasePaused, lc.GetDesiredPhase())
}

func Test_GetPipelineLimits(t *testing.T) {
	pl := Pipeline{
		Spec: PipelineSpec{},
	}
	l := pl.GetPipelineLimits()
	assert.Equal(t, int64(DefaultBufferLength), int64(*l.BufferMaxLength))
	assert.Equal(t, float64(DefaultBufferUsageLimit), float64(*l.BufferUsageLimit)/100)
	assert.Equal(t, int64(DefaultReadBatchSize), int64(*l.ReadBatchSize))
	assert.Equal(t, "1s", l.ReadTimeout.Duration.String())

	length := uint64(2000)
	usuageLimit := uint32(40)
	readBatch := uint64(321)
	pl.Spec.Limits = &PipelineLimits{
		BufferMaxLength:  &length,
		BufferUsageLimit: &usuageLimit,
		ReadBatchSize:    &readBatch,
		ReadTimeout:      &metav1.Duration{Duration: time.Duration(5 * time.Second)},
	}
	l = pl.GetPipelineLimits()
	assert.Equal(t, length, *l.BufferMaxLength)
	assert.Equal(t, float64(40)/100, float64(*l.BufferUsageLimit)/100)
	assert.Equal(t, readBatch, *l.ReadBatchSize)
	assert.Equal(t, "5s", l.ReadTimeout.Duration.String())
}

func Test_GetAllBuckets(t *testing.T) {
	pl := Pipeline{
		Spec: PipelineSpec{
			Vertices: []AbstractVertex{
				{Name: "input", Source: &Source{}},
				{Name: "p1"},
				{Name: "p2"},
				{Name: "p11"},
				{Name: "output", Sink: &Sink{}},
			},
			Edges: []Edge{
				{From: "input", To: "p1"},
				{From: "p1", To: "p11"},
				{From: "p1", To: "p2"},
				{From: "p2", To: "output"},
			},
		},
	}
	buckets := pl.GetAllBuckets()
	assert.Equal(t, 6, len(buckets))
}

func Test_FindVertexWithBuffer(t *testing.T) {
	v := testPipeline.FindVertexWithBuffer(GenerateBufferName(testNamespace, testPipelineName, "p1", 0))
	assert.NotNil(t, v)
}

func Test_GetSideInputManagerDeployments(t *testing.T) {
	t.Run("side inputs not enabled", func(t *testing.T) {
		deployments, err := testPipeline.GetSideInputsManagerDeployments(testGetSideInputDeploymentReq)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(deployments))
	})

	t.Run("side inputs enabled", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.SideInputs = []SideInput{
			{
				Name: "side-input-1",
				Container: &Container{
					Image: "side-input-1",
				},
				Trigger: &SideInputTrigger{
					Schedule: "0 0 * * *",
				},
			},
		}
		deployments, err := testObj.GetSideInputsManagerDeployments(testGetSideInputDeploymentReq)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(deployments))
		assert.Equal(t, 2, len(deployments[0].Spec.Template.Spec.Containers))
	})
}
