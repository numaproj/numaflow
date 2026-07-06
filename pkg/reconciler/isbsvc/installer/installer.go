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

package installer

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
)

// Installer is an interface for ISB Service installation
type Installer interface {
	Install(ctx context.Context) (*dfv1.BufferServiceConfig, error)
	// Uninstall only needs to handle those resources not cascade deleted.
	// For example, undeleted PVCs not automatically deleted when deleting a StatefulSet
	Uninstall(ctx context.Context) error
	// CheckChildrenResourceStatus checks the status of the resources created by the ISB Service.
	// Returns (needsRequeue, error): needsRequeue is true when the unhealthy state is transient
	// (e.g. a pod had a recent restart) and the controller must explicitly requeue to re-evaluate
	// once the transient window expires, since no Kubernetes event will trigger it.
	CheckChildrenResourceStatus(ctx context.Context) (bool, error)
}

// Install installs the ISB Service and checks child resource status.
// Returns (needsRequeue, error): needsRequeue mirrors CheckChildrenResourceStatus's signal
// for transient pod failures that require an explicit requeue.
func Install(ctx context.Context, isbSvc *dfv1.InterStepBufferService, client client.Client, kubeClient kubernetes.Interface, config *reconciler.GlobalConfig, logger *zap.SugaredLogger, recorder record.EventRecorder) (bool, error) {
	installer, err := getInstaller(isbSvc, client, kubeClient, config, logger, recorder)
	if err != nil {
		logger.Errorw("failed to get an installer", zap.Error(err))
		return false, err
	}
	bufferConfig, err := installer.Install(ctx)
	if err != nil {
		logger.Errorw("installation error", zap.Error(err))
		return false, err
	}
	needsRequeue, err := installer.CheckChildrenResourceStatus(ctx)
	if err != nil {
		logger.Errorw("failed to check children resource status", zap.Error(err))
		return false, err
	}
	isbSvc.Status.Config = *bufferConfig

	return needsRequeue, nil
}

// GetInstaller returns Installer implementation
func getInstaller(isbSvc *dfv1.InterStepBufferService, client client.Client, kubeClient kubernetes.Interface, config *reconciler.GlobalConfig, logger *zap.SugaredLogger, recorder record.EventRecorder) (Installer, error) {
	labels := map[string]string{
		dfv1.KeyPartOf:     dfv1.Project,
		dfv1.KeyManagedBy:  dfv1.ControllerISBSvc,
		dfv1.KeyComponent:  dfv1.ComponentISBSvc,
		dfv1.KeyISBSvcName: isbSvc.Name,
	}
	if js := isbSvc.Spec.JetStream; js != nil {
		labels[dfv1.KeyISBSvcType] = string(dfv1.ISBSvcTypeJetStream)
		return NewJetStreamInstaller(client, kubeClient, isbSvc, config, labels, logger, recorder), nil
	}
	return nil, fmt.Errorf("invalid isb service spec")
}

// Uninstall function will be run before the ISB Service object is deleted,
// usually it could be used to uninstall the extra resources who would not be cleaned
// up when an ISB Service is deleted. Most of the time this is not needed as all
// the dependency resources should have been deleted by owner references cascade
// deletion, but things like PVC created by StatefulSet need to be cleaned up
// separately.
//
// It could also be used to check if the ISB Service object can be safely deleted.
func Uninstall(ctx context.Context, isbSvc *dfv1.InterStepBufferService, client client.Client, kubeClient kubernetes.Interface, config *reconciler.GlobalConfig, logger *zap.SugaredLogger, recorder record.EventRecorder) error {
	pls, err := referencedPipelines(ctx, client, isbSvc)
	if err != nil {
		return fmt.Errorf("failed to check if there is any pipeline using this InterStepBufferService, %w", err)
	}
	if pls > 0 {
		return fmt.Errorf("can not delete InterStepBufferService %q which has %d pipelines connected", isbSvc.Name, pls)
	}
	installer, err := getInstaller(isbSvc, client, kubeClient, config, logger, recorder)
	if err != nil {
		logger.Errorw("Failed to get an installer", zap.Error(err))
		return err
	}
	return installer.Uninstall(ctx)
}

func referencedPipelines(ctx context.Context, c client.Client, isbSvc *dfv1.InterStepBufferService) (int, error) {
	pipelines := &dfv1.PipelineList{}
	if err := c.List(ctx, pipelines, &client.ListOptions{
		Namespace: isbSvc.Namespace,
	}); err != nil {
		return 0, err
	}
	result := 0
	for _, pl := range pipelines.Items {
		isbSvcName := pl.Spec.InterStepBufferServiceName
		if isbSvcName == "" {
			isbSvcName = dfv1.DefaultISBSvcName
		}
		if isbSvcName == isbSvc.Name {
			result++
		}
	}
	return result, nil
}
