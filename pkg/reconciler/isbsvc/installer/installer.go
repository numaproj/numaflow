package installer

import (
	"context"
	"fmt"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"go.uber.org/zap"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Installer is an interface for ISBS installation
type Installer interface {
	Install(ctx context.Context) (*dfv1.BufferServiceConfig, error)
	// Uninstall only needs to handle those resources not cascade deleted.
	// For example, undeleted PVCs not automatically deleted when deleting a StatefulSet
	Uninstall(ctx context.Context) error
}

// Install function installs the ISBS
func Install(ctx context.Context, isbsvc *dfv1.InterStepBufferService, client client.Client, config *reconciler.GlobalConfig, logger *zap.SugaredLogger) error {
	installer, err := getInstaller(isbsvc, client, config, logger)
	if err != nil {
		logger.Errorw("failed to get an installer", zap.Error(err))
		return err
	}
	bufferConfig, err := installer.Install(ctx)
	if err != nil {
		logger.Errorw("installation error", zap.Error(err))
		return err
	}
	isbsvc.Status.Config = *bufferConfig
	return nil
}

// GetInstaller returns Installer implementation
func getInstaller(isbsvc *dfv1.InterStepBufferService, client client.Client, config *reconciler.GlobalConfig, logger *zap.SugaredLogger) (Installer, error) {
	labels := map[string]string{
		dfv1.KeyPartOf:     dfv1.Project,
		dfv1.KeyManagedBy:  dfv1.ControllerISBSvc,
		dfv1.KeyComponent:  dfv1.ComponentISBSvc,
		dfv1.KeyISBSvcName: isbsvc.Name,
	}
	if redis := isbsvc.Spec.Redis; redis != nil {
		labels[dfv1.KeyISBSvcType] = string(dfv1.ISBSvcTypeRedis)
		if redis.External != nil {
			return NewExternalRedisInstaller(isbsvc, logger), nil
		} else if redis.Native != nil {
			return NewNativeRedisInstaller(client, isbsvc, config, labels, logger), nil
		}
	} else if js := isbsvc.Spec.JetStream; js != nil {
		labels[dfv1.KeyISBSvcType] = string(dfv1.ISBSvcTypeJetStream)
		return NewJetStreamInstaller(client, isbsvc, config, labels, logger), nil
	}
	return nil, fmt.Errorf("invalid isb service spec")
}

// Uninstall function will be run before the ISBS object is deleted,
// usually it could be used to uninstall the extra resources who would not be cleaned
// up when an ISBS is deleted. Most of the time this is not needed as all
// the dependency resources should have been deleted by owner references cascade
// deletion, but things like PVC created by StatefulSet need to be cleaned up
// separately.
//
// It could also be used to check if the ISB Service object can be safely deleted.
func Uninstall(ctx context.Context, isbsvc *dfv1.InterStepBufferService, client client.Client, config *reconciler.GlobalConfig, logger *zap.SugaredLogger) error {
	installer, err := getInstaller(isbsvc, client, config, logger)
	if err != nil {
		logger.Errorw("Failed to get an installer", zap.Error(err))
		return err
	}
	return installer.Uninstall(ctx)
}
