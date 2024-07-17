package pipeline

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// DeploymentStatusHandler updates the Pipeline status based on the Deployment status
func DeploymentStatusHandler(mgr manager.Manager, logger *zap.SugaredLogger) handler.EventHandler {
	return handler.Funcs{
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
			// Check if the resource version has changed
			if e.ObjectOld.GetResourceVersion() == e.ObjectNew.GetResourceVersion() {
				return
			}

			// Fetch the owner references from the metadata
			ownerRefs := e.ObjectNew.GetOwnerReferences()
			ownedByPipeline := false
			for _, ref := range ownerRefs {
				if ref.Kind == dfv1.PipelineGroupVersionKind.Kind && ref.APIVersion == dfv1.PipelineGroupVersionKind.GroupVersion().String() {
					ownedByPipeline = true
					break
				}
			}
			// Proceed only if the Pipeline owns the resources.
			if !ownedByPipeline {
				return
			}

			deploymentName := e.ObjectNew.GetName()
			deploymentNamespace := e.ObjectNew.GetNamespace()
			log := logger.With("namespace", deploymentNamespace).With("pipeline", deploymentName)

			var deployment appv1.Deployment
			if err := mgr.GetClient().Get(context.TODO(), client.ObjectKey{
				Namespace: deploymentNamespace,
				Name:      deploymentName,
			}, &deployment); err != nil {
				log.Errorw("Unable to fetch Deployment for Pipeline", zap.Error(err))
				return
			}

			// fetch the latest Pipeline for updating the status
			var pipeline dfv1.Pipeline
			if err := mgr.GetClient().Get(context.TODO(), client.ObjectKey{
				Namespace: deploymentNamespace,                          // Deployment and Pipeline are in the same namespace
				Name:      deployment.GetLabels()[dfv1.KeyPipelineName], // Pipeline name is stored in the Deployment labels
			}, &pipeline); err != nil {
				log.Errorw("Unable to fetch Pipeline", zap.Error(err))
				return
			}

			if msg, serviceType, reason, status := getDeploymentStatus(&deployment); status {
				pipeline.Status.MarkServiceHealthy(serviceType, reason, msg)
			} else {
				pipeline.Status.MarkServiceNotHealthy(serviceType, reason, msg)
			}
			// patch the status of pipeline
			if err := mgr.GetClient().Status().Patch(ctx, &pipeline, client.Merge); err != nil {
				logger.Errorw("Unable to update Pipeline status", zap.Error(err))
				return
			}
		},
	}
}

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
	return fmt.Sprintf("Waiting for deployment spec update to be observed...\n"), serviceType, "DeploymentNotComplete", false
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
