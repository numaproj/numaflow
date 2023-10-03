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
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func TestExternalRedisInstallation(t *testing.T) {
	t.Run("bad installation", func(t *testing.T) {
		badIsbs := testExternalRedisIsbSvc.DeepCopy()
		badIsbs.Spec.Redis = nil
		installer := &externalRedisInstaller{
			isbs:   badIsbs,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		_, err := installer.Install(context.TODO())
		assert.Error(t, err)
	})

	t.Run("good installation", func(t *testing.T) {
		goodIsbs := testExternalRedisIsbSvc.DeepCopy()
		installer := &externalRedisInstaller{
			isbs:   goodIsbs,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		c, err := installer.Install(context.TODO())
		assert.NoError(t, err)
		assert.NotNil(t, c.Redis)
		assert.Equal(t, goodIsbs.Spec.Redis.External.URL, c.Redis.URL)
	})
}

func TestExternalRedisUninstallation(t *testing.T) {
	obj := testExternalRedisIsbSvc.DeepCopy()
	installer := &externalRedisInstaller{
		isbs:   obj,
		logger: zaptest.NewLogger(t).Sugar(),
	}
	err := installer.Uninstall(context.TODO())
	assert.NoError(t, err)
}
