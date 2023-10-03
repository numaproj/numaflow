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
