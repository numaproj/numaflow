package pipeline

import (
	"context"
	"fmt"

	appv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// getVertexStatus will calculate the status of the vertices and return the status and reason
func getVertexStatus(ctx context.Context, c client.Client, pipeline *dfv1.Pipeline) (string, string, error) {
	totalVertices := len(pipeline.Spec.Vertices)
	healthyVertices := 0
	var vertices dfv1.VertexList
	if err := c.List(ctx, &vertices, client.InNamespace(pipeline.GetNamespace()),
		client.MatchingLabels{dfv1.KeyPipelineName: pipeline.GetName()}); err != nil {
		return "", "", err
	}
	for _, vertex := range vertices.Items {
		for _, condition := range vertex.Status.Conditions {
			if condition.Type == string(dfv1.VertexConditionPodHealthy) {
				if condition.Status == "True" {
					healthyVertices++
				}
			}
		}
	}
	if healthyVertices == totalVertices {
		return "True", "Successful", nil
	} else {
		return "False", "Progressing", nil
	}
}

// getDeploymentStatus returns a message describing deployment status, and message with reason where bool value
// indicating if the status is considered done.
// Borrowed at kubernetes/kubectl/rollout_status.go https://github.com/kubernetes/kubernetes/blob/cea1d4e20b4a7886d8ff65f34c6d4f95efcb4742/staging/src/k8s.io/kubectl/pkg/polymorphichelpers/rollout_status.go#L59
func getDeploymentStatus(deployment *appv1.Deployment) (string, string, bool) {
	if deployment.Generation <= deployment.Status.ObservedGeneration {
		cond := getDeploymentCondition(deployment.Status, appv1.DeploymentProgressing)
		if cond != nil && cond.Reason == "ProgressDeadlineExceeded" {
			return fmt.Sprintf("deployment %q exceeded its progress deadline", deployment.Name), "ProgressDeadlineExceeded", false
		}
		if deployment.Spec.Replicas != nil && deployment.Status.UpdatedReplicas < *deployment.Spec.Replicas {
			return fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d out of %d new replicas have been updated...\n",
				deployment.Name, deployment.Status.UpdatedReplicas, *deployment.Spec.Replicas), "DeploymentNotComplete", false
		}
		if deployment.Status.Replicas > deployment.Status.UpdatedReplicas {
			return fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d old replicas are pending termination...\n",
				deployment.Name, deployment.Status.Replicas-deployment.Status.UpdatedReplicas), "DeploymentNotComplete", false
		}
		if deployment.Status.AvailableReplicas < deployment.Status.UpdatedReplicas {
			return fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d of %d updated replicas are available...\n",
				deployment.Name, deployment.Status.AvailableReplicas, deployment.Status.UpdatedReplicas), "DeploymentNotComplete", false
		}
		return fmt.Sprintf("deployment %q successfully rolled out\n", deployment.Name), "DeploymentComplete", true
	}
	return "Waiting for deployment spec update to be observed...", "DeploymentNotComplete", false
}

// GetDeploymentCondition returns the condition with the provided type.
func getDeploymentCondition(status appv1.DeploymentStatus, condType appv1.DeploymentConditionType) *appv1.DeploymentCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}
