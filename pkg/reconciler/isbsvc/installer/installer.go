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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Installer is an interface for ISB Service installation
type Installer interface {
	Install(ctx context.Context) (*dfv1.BufferServiceConfig, error)
	// Uninstall only needs to handle those resources not cascade deleted.
	// For example, undeleted PVCs not automatically deleted when deleting a StatefulSet
	Uninstall(ctx context.Context) error
}

// Install function installs the ISB Service
func Install(ctx context.Context, isbSvc *dfv1.InterStepBufferService, client client.Client, kubeClient kubernetes.Interface, config *reconciler.GlobalConfig, logger *zap.SugaredLogger, recorder record.EventRecorder) error {
	installer, err := getInstaller(isbSvc, client, kubeClient, config, logger, recorder)
	if err != nil {
		logger.Errorw("failed to get an installer", zap.Error(err))
		return err
	}
	bufferConfig, err := installer.Install(ctx)
	if err != nil {
		logger.Errorw("installation error", zap.Error(err))
		return err
	}
	isbSvc.Status.Config = *bufferConfig

	return nil
}

// GetInstaller returns Installer implementation
func getInstaller(isbSvc *dfv1.InterStepBufferService, client client.Client, kubeClient kubernetes.Interface, config *reconciler.GlobalConfig, logger *zap.SugaredLogger, recorder record.EventRecorder) (Installer, error) {
	labels := map[string]string{
		dfv1.KeyPartOf:     dfv1.Project,
		dfv1.KeyManagedBy:  dfv1.ControllerISBSvc,
		dfv1.KeyComponent:  dfv1.ComponentISBSvc,
		dfv1.KeyISBSvcName: isbSvc.Name,
	}
	if redis := isbSvc.Spec.Redis; redis != nil {
		labels[dfv1.KeyISBSvcType] = string(dfv1.ISBSvcTypeRedis)
		if redis.External != nil {
			return NewExternalRedisInstaller(isbSvc, logger), nil
		} else if redis.Native != nil {
			return NewNativeRedisInstaller(client, kubeClient, isbSvc, config, labels, logger, recorder), nil
		}
	} else if js := isbSvc.Spec.JetStream; js != nil {
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
	installer, err := getInstaller(isbSvc, client, kubeClient, config, logger, recorder)
	if err != nil {
		logger.Errorw("Failed to get an installer", zap.Error(err))
		return err
	}
	return installer.Uninstall(ctx)
}
