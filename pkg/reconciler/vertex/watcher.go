package vertex

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func PodsStatusHandler(mgr manager.Manager, logger *zap.SugaredLogger) handler.EventHandler {
	return handler.Funcs{
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
			// Check if the resource version has changed
			if e.ObjectOld.GetResourceVersion() == e.ObjectNew.GetResourceVersion() {
				return
			}

			// Fetch the owner references from the metadata
			ownerRefs := e.ObjectNew.GetOwnerReferences()
			ownedByVertex := false
			for _, ref := range ownerRefs {
				if ref.Kind == dfv1.VertexGroupVersionKind.Kind && ref.APIVersion == dfv1.VertexGroupVersionKind.GroupVersion().String() {
					ownedByVertex = true
					break
				}
			}
			// Proceed only if the vertex owns the resources.
			if !ownedByVertex {
				return
			}

			vertexPodName := e.ObjectNew.GetName()
			vertexPodNamespace := e.ObjectNew.GetNamespace()
			log := logger.With("namespace", vertexPodNamespace).With("vertex pod", vertexPodName)

			// fetch the pods for calculating the status of child resources
			var podList corev1.PodList
			if err := mgr.GetClient().List(context.TODO(), &podList,
				client.InNamespace(vertexPodNamespace),
				client.MatchingLabels{dfv1.KeyVertexName: e.ObjectNew.GetLabels()[dfv1.KeyVertexName],
					dfv1.KeyPipelineName: e.ObjectNew.GetLabels()[dfv1.KeyPipelineName]},
			); err != nil {
				log.Errorw("Unable to fetch Pods for Vertex", zap.Error(err))
				return
			}

			// fetch the latest Vertex for updating the status
			var vertex dfv1.Vertex
			if err := mgr.GetClient().Get(context.TODO(), client.ObjectKey{
				Namespace: vertexPodNamespace, // Vertex is in the same namespace as the pods
				Name:      e.ObjectNew.GetLabels()[dfv1.KeyPipelineName] + "-" + e.ObjectNew.GetLabels()[dfv1.KeyVertexName],
			}, &vertex); err != nil {
				log.Errorw("Unable to fetch Vertex", zap.Error(err))
				return
			}

			if msg, reason, status := getVertexStatus(&vertex, &podList); status {
				vertex.Status.MarkPodHealthy(reason, msg)
			} else {
				vertex.Status.MarkPodNotHealthy(reason, msg)
			}

			if err := mgr.GetClient().Status().Update(ctx, &vertex); err != nil {
				logger.Errorw("Unable to update Vertex status", zap.Error(err))
				return
			}
		},
	}
}

// getVertexStatus calculate the status by iterating over pods objects
// TODO: Handle "Cooldown period" for the vertex pods where no any pod is available.
func getVertexStatus(vertex *dfv1.Vertex, pods *corev1.PodList) (string, string, bool) {
	if len(pods.Items) == 0 {
		return "No Pods found", "PodNotFound", false
	} else if *vertex.Spec.Replicas != int32(len(pods.Items)) {
		return "Number of pods are not equal to replicas", "Processing", false
	} else {
		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodRunning {
				return fmt.Sprintf("Pod %s is not in running state", pod.Name), "Processing", false
			}
		}
	}

	return "All vertex pods are healthy", "Running", true
}
