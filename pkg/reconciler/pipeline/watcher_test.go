package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

var (
	replicas   int32 = 1
	deployment       = appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment",
			Namespace: "default",
		},
		Spec: appv1.DeploymentSpec{
			Replicas: &replicas,
		},
		Status: appv1.DeploymentStatus{
			ObservedGeneration: 1,
			UpdatedReplicas:    1,
			Replicas:           1,
			AvailableReplicas:  1,
		},
	}
)

func TestGetDeploymentStatus(t *testing.T) {
	t.Run("Test Deployment status as true", func(t *testing.T) {
		testDeployment := deployment.DeepCopy()
		message, reason, done := getDeploymentStatus(testDeployment)
		assert.Equal(t, "DeploymentComplete", reason)
		assert.True(t, done)
		assert.Equal(t, "deployment \"test-deployment\" successfully rolled out\n", message)
	})

	t.Run("Test Deployment status as false", func(t *testing.T) {
		testDeployment := deployment.DeepCopy()
		testDeployment.Status.ObservedGeneration = 0
		testDeployment.Status.UpdatedReplicas = 0
		message, reason, done := getDeploymentStatus(testDeployment)
		assert.Equal(t, "DeploymentNotComplete", reason)
		assert.False(t, done)
		assert.Equal(t, "Waiting for deployment \"test-deployment\" rollout to finish: 0 out of 1 new replicas have been updated...\n", message)
	})

	t.Run("Test deployment status as false while updating replica", func(t *testing.T) {
		testDeployment := deployment.DeepCopy()
		testDeployment.Status.UpdatedReplicas = 1
		testDeployment.Status.Replicas = 2
		message, reason, done := getDeploymentStatus(testDeployment)
		assert.Equal(t, "DeploymentNotComplete", reason)
		assert.False(t, done)
		assert.Equal(t, "Waiting for deployment \"test-deployment\" rollout to finish: 1 old replicas are pending termination...\n", message)
	})
}

func TestGetVertexStatus(t *testing.T) {
	t.Run("Test Vertex status as true", func(t *testing.T) {
		vertices := dfv1.VertexList{
			Items: []dfv1.Vertex{
				{
					Status: dfv1.VertexStatus{
						Phase: "Running",
					},
				},
			},
		}
		vertices.Items[0].Status.Conditions = []metav1.Condition{
			{
				Type:   string(dfv1.VertexConditionPodsHealthy),
				Status: metav1.ConditionTrue,
			},
		}
		status, reason := getVertexStatus(&vertices)
		assert.True(t, status)
		assert.Equal(t, "Successful", reason)
	})

	t.Run("Test Vertex status as false", func(t *testing.T) {
		vertices := dfv1.VertexList{
			Items: []dfv1.Vertex{
				{
					Status: dfv1.VertexStatus{
						Phase: "Pending",
					},
				},
			},
		}
		vertices.Items[0].Status.Conditions = []metav1.Condition{
			{
				Type:   string(dfv1.VertexConditionPodsHealthy),
				Status: metav1.ConditionTrue,
			},
		}
		status, reason := getVertexStatus(&vertices)
		assert.False(t, status)
		assert.Equal(t, "Progressing", reason)
	})
}
