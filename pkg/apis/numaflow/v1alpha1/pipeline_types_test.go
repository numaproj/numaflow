package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	assert.Contains(t, s, testPipeline.Namespace+"-"+testPipeline.Name+"-input-p1")
	assert.Contains(t, s, testPipeline.Namespace+"-"+testPipeline.Name+"-p1-output")
}

func Test_GetVertex(t *testing.T) {
	v := testPipeline.GetVertex("abc")
	assert.Nil(t, v)
	v = testPipeline.GetVertex("input")
	assert.NotNil(t, v)
}

func Test_FindVertexWithBuffer(t *testing.T) {
	vFrom, vTo := testPipeline.FindVerticesWithBuffer("nonono")
	assert.Nil(t, vFrom)
	assert.Nil(t, vTo)
	vFrom, vTo = testPipeline.FindVerticesWithBuffer(testPipeline.Namespace + "-" + testPipeline.Name + "-input-p1")
	assert.NotNil(t, vFrom)
	assert.Equal(t, "input", vFrom.Name)
	assert.NotNil(t, vTo)
	assert.Equal(t, "p1", vTo.Name)
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

	t.Run("test get init container", func(t *testing.T) {
		c := testPipeline.getDaemonPodInitContainer(req)
		assert.Equal(t, CtrInit, c.Name)
		assert.Equal(t, req.Image, c.Image)
		assert.Contains(t, c.Args, "isbsvc-buffer-validate")
	})
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
