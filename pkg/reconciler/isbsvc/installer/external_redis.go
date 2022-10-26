package installer

import (
	"context"
	"fmt"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"go.uber.org/zap"
)

type externalRedisInstaller struct {
	isbs   *dfv1.InterStepBufferService
	logger *zap.SugaredLogger
}

func NewExternalRedisInstaller(isbs *dfv1.InterStepBufferService, logger *zap.SugaredLogger) Installer {
	return &externalRedisInstaller{
		isbs:   isbs,
		logger: logger.With("isbs", isbs.Name),
	}
}

func (eri *externalRedisInstaller) Install(ctx context.Context) (*dfv1.BufferServiceConfig, error) {
	if eri.isbs.Spec.Redis == nil || eri.isbs.Spec.Redis.External == nil {
		return nil, fmt.Errorf("invalid InterStepBufferService spec, no external config")
	}
	eri.isbs.Status.SetType(dfv1.ISBSvcTypeRedis)
	eri.isbs.Status.MarkConfigured()
	eri.isbs.Status.MarkDeployed()
	eri.logger.Info("Using external redis config")
	return &dfv1.BufferServiceConfig{Redis: eri.isbs.Spec.Redis.External}, nil
}

func (eri *externalRedisInstaller) Uninstall(ctx context.Context) error {
	eri.logger.Info("Nothing to uninstall")
	return nil
}
