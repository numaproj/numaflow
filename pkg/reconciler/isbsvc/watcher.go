package isbsvc

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

// StatefulSetStatusHandler updates the InterStepBufferService status based on the StatefulSet status
func StatefulSetStatusHandler(mgr manager.Manager, logger *zap.SugaredLogger) handler.EventHandler {
	return handler.Funcs{
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.RateLimitingInterface) {
			// Check if the resource version has changed
			if e.ObjectOld.GetResourceVersion() == e.ObjectNew.GetResourceVersion() {
				return
			}

			// Fetch the owner references from the metadata
			ownerRefs := e.ObjectNew.GetOwnerReferences()
			ownedByISB := false
			for _, ref := range ownerRefs {
				if ref.Kind == dfv1.ISBGroupVersionKind.Kind && ref.APIVersion == dfv1.ISBGroupVersionKind.GroupVersion().String() {
					ownedByISB = true
					break
				}
			}
			// Proceed only if ISB service owns the resource
			if !ownedByISB {
				return
			}

			isbStsName := e.ObjectNew.GetName()
			isbStsNamespace := e.ObjectNew.GetNamespace()
			log := logger.With("namespace", isbStsNamespace).With("ISB Stateful set", isbStsName)

			// fetch the StatefulSet for calculating the status of child resources
			var isbStatefulSet appv1.StatefulSet
			if err := mgr.GetClient().Get(context.TODO(), client.ObjectKey{
				Namespace: isbStsNamespace,
				Name:      isbStsName,
			}, &isbStatefulSet); err != nil {
				log.Errorw("Unable to fetch StatefulSet for InterStepBufferService", zap.Error(err))
				return
			}

			// fetch the latest InterStepBufferService for updating the status
			var isbSvc dfv1.InterStepBufferService
			if err := mgr.GetClient().Get(context.TODO(), client.ObjectKey{
				Namespace: isbStsNamespace,                           // StatefulSet and InterStepBufferService are in the same namespace
				Name:      isbStatefulSet.Labels[dfv1.KeyISBSvcName], // InterStepBufferService name is stored in the StatefulSet labels
			}, &isbSvc); err != nil {
				log.Errorw("Unable to fetch InterStepBufferService", zap.Error(err))
				return
			}
			// calculate the status of the InterStepBufferService by statefulset status and update the status of isbSvc
			if msg, status := getStatefulSetStatus(&isbStatefulSet); status {
				isbSvc.Status.MarkChildHealthy("MinimumReplicasAvailable", msg)
			} else {
				isbSvc.Status.MarkChildNotHealthy("MinimumReplicasAvailable", msg)
			}
			// patch the status of isbSvc
			if err := mgr.GetClient().Status().Patch(ctx, &isbSvc, client.Merge); err != nil {
				log.Errorw("Unable to update InterStepBufferService status", zap.Error(err))
				return
			}
		},
	}
}

// getStatefulSetStatus returns a message describing statefulset status, and a bool value indicating if the status is considered done.
// Borrowed at kubernetes/kubectl/rollout_status.go https://github.com/kubernetes/kubernetes/blob/cea1d4e20b4a7886d8ff65f34c6d4f95efcb4742/staging/src/k8s.io/kubectl/pkg/polymorphichelpers/rollout_status.go#L130
func getStatefulSetStatus(sts *appv1.StatefulSet) (string, bool) {
	if sts.Status.ObservedGeneration == 0 || sts.Generation > sts.Status.ObservedGeneration {
		return "Waiting for statefulset spec update to be observed...\n", false
	}
	if sts.Spec.Replicas != nil && sts.Status.ReadyReplicas < *sts.Spec.Replicas {
		return fmt.Sprintf("Waiting for %d pods to be ready...\n", *sts.Spec.Replicas-sts.Status.ReadyReplicas), false
	}
	if sts.Spec.UpdateStrategy.Type == appv1.RollingUpdateStatefulSetStrategyType && sts.Spec.UpdateStrategy.RollingUpdate != nil {
		if sts.Spec.Replicas != nil && sts.Spec.UpdateStrategy.RollingUpdate.Partition != nil {
			if sts.Status.UpdatedReplicas < (*sts.Spec.Replicas - *sts.Spec.UpdateStrategy.RollingUpdate.Partition) {
				return fmt.Sprintf(
					"Waiting for partitioned roll out to finish: %d out of %d new pods have been updated...\n",
					sts.Status.UpdatedReplicas, *sts.Spec.Replicas-*sts.Spec.UpdateStrategy.RollingUpdate.Partition), false
			}
		}
		return fmt.Sprintf("partitioned roll out complete: %d new pods have been updated...\n",
			sts.Status.UpdatedReplicas), true
	}
	if sts.Status.UpdateRevision != sts.Status.CurrentRevision {
		return fmt.Sprintf("waiting for statefulset rolling update to complete %d pods at revision %s...\n",
			sts.Status.UpdatedReplicas, sts.Status.UpdateRevision), false
	}
	return fmt.Sprintf(
		"statefulset rolling update complete %d pods at revision %s...\n",
		sts.Status.CurrentReplicas, sts.Status.CurrentRevision), true
}
