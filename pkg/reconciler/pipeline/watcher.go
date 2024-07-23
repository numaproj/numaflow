package pipeline

import (
	"context"
	"fmt"

	appv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func setResourceStatus(c client.Client, pipeline *dfv1.Pipeline) error {
	// get the daemon deployment and update the status of it to the pipeline
	var daemonDeployment appv1.Deployment
	if err := c.Get(context.TODO(), client.ObjectKey{
		Namespace: pipeline.GetNamespace(),
		Name:      pipeline.GetDaemonDeploymentName(),
	}, &daemonDeployment); err != nil {
		return err
	}
	if msg, serviceType, reason, status := getDeploymentStatus(&daemonDeployment); status {
		pipeline.Status.MarkServiceHealthy(serviceType, reason, msg)
	} else {
		pipeline.Status.MarkServiceNotHealthy(serviceType, reason, msg)
	}

	// get the side input deployments and update the status of them to the pipeline
	for _, sideInput := range pipeline.Spec.SideInputs {
		var sideInputDeployment appv1.Deployment
		if err := c.Get(context.TODO(), client.ObjectKey{
			Namespace: pipeline.GetNamespace(),
			Name:      pipeline.GetSideInputsManagerDeploymentName(sideInput.Name),
		}, &sideInputDeployment); err != nil {
			return err
		}

		if msg, serviceType, reason, status := getDeploymentStatus(&sideInputDeployment); status {
			pipeline.Status.MarkServiceHealthy(serviceType, reason, msg)
		} else {
			pipeline.Status.MarkServiceNotHealthy(serviceType, reason, msg)

		}

	}

	// calculate the status of the vertices and update the status of them to the pipeline
	status, reason, err := getVertexStatus(c, pipeline)
	if err != nil {
		return err
	}
	if status == "True" {
		pipeline.Status.MarkServiceHealthy(dfv1.PipelineConditionVerticesServiceHealthy, reason, "All Vertices are healthy")
	} else if status == "False" {
		pipeline.Status.MarkServiceNotHealthy(dfv1.PipelineConditionVerticesServiceHealthy, reason, "Some Vertices are not healthy")
	}

	return nil
}

// getVertexStatus will calculate the status of the vertices and return the status and reason
func getVertexStatus(c client.Client, pipeline *dfv1.Pipeline) (string, string, error) {
	totalVertices := len(pipeline.Spec.Vertices)
	healthyVertices := 0
	for _, pipelineVertex := range pipeline.Spec.Vertices {
		var vertex dfv1.Vertex
		if err := c.Get(context.TODO(), client.ObjectKey{
			Namespace: pipeline.GetNamespace(),
			Name:      pipeline.GetVertexName(pipelineVertex.Name),
		}, &vertex); err != nil {
			return "", "", err
		}
		for _, condition := range vertex.Status.Conditions {
			if condition.Type == string(dfv1.VertexConditionPodHealthy) {
				if condition.Status == "True" {
					healthyVertices++
				}
			}
		}
	}

	if healthyVertices == totalVertices {
		return "True", "Progressing", nil
	} else {
		return "False", "Progressing", nil
	}
}

// getDeploymentStatus returns a message describing deployment status, and message with service type and reason where
// bool value indicating if the status is considered done.
// Borrowed at kubernetes/kubectl/rollout_status.go https://github.com/kubernetes/kubernetes/blob/cea1d4e20b4a7886d8ff65f34c6d4f95efcb4742/staging/src/k8s.io/kubectl/pkg/polymorphichelpers/rollout_status.go#L59
func getDeploymentStatus(deployment *appv1.Deployment) (string, dfv1.ConditionType, string, bool) {
	// get the service type using component label value
	var serviceType dfv1.ConditionType
	if deployment.GetLabels()[dfv1.KeyComponent] == dfv1.ComponentDaemon {
		serviceType = dfv1.PipelineConditionDaemonServiceHealthy
	} else {
		serviceType = dfv1.PipelineConditionSideInputServiceHealthy
	}

	if deployment.Generation <= deployment.Status.ObservedGeneration {
		cond := getDeploymentCondition(deployment.Status, appv1.DeploymentProgressing)
		if cond != nil && cond.Reason == "ProgressDeadlineExceeded" {
			return fmt.Sprintf("deployment %q exceeded its progress deadline", deployment.Name), serviceType, "ProgressDeadlineExceeded", false
		}
		if deployment.Spec.Replicas != nil && deployment.Status.UpdatedReplicas < *deployment.Spec.Replicas {
			return fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d out of %d new replicas have been updated...\n",
				deployment.Name, deployment.Status.UpdatedReplicas, *deployment.Spec.Replicas), serviceType, "DeploymentNotComplete", false
		}
		if deployment.Status.Replicas > deployment.Status.UpdatedReplicas {
			return fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d old replicas are pending termination...\n",
				deployment.Name, deployment.Status.Replicas-deployment.Status.UpdatedReplicas), serviceType, "DeploymentNotComplete", false
		}
		if deployment.Status.AvailableReplicas < deployment.Status.UpdatedReplicas {
			return fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d of %d updated replicas are available...\n",
				deployment.Name, deployment.Status.AvailableReplicas, deployment.Status.UpdatedReplicas), serviceType, "DeploymentNotComplete", false
		}
		return fmt.Sprintf("deployment %q successfully rolled out\n", deployment.Name), serviceType, "DeploymentComplete", true
	}
	return "Waiting for deployment spec update to be observed...", serviceType, "DeploymentNotComplete", false
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
