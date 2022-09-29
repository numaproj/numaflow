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
