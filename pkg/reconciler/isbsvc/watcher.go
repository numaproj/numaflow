package isbsvc

import (
	"context"
	"fmt"

	appv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func setStatefulSetStatus(c client.Client, isbSvc *dfv1.InterStepBufferService) error {
	var isbStatefulSet appv1.StatefulSet
	if err := c.Get(context.TODO(), client.ObjectKey{
		Namespace: isbSvc.Namespace,
		Name:      fmt.Sprintf("isbsvc-%s-js", isbSvc.Name),
	}, &isbStatefulSet); err != nil {
		return err
	}

	// calculate the status of the InterStepBufferService by statefulset status and update the status of isbSvc
	if msg, status := getStatefulSetStatus(&isbStatefulSet); status {
		isbSvc.Status.MarkChildHealthy("MinimumReplicasAvailable", msg)
	} else {
		isbSvc.Status.MarkChildNotHealthy("MinimumReplicasAvailable", msg)
	}

	return nil
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
