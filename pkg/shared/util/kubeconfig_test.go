package util

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestK8sRestConfig(t *testing.T) {
	t.Run("K8sRestConfig returns error when KUBECONFIG is invalid", func(t *testing.T) {
		// Setup the environment to simulate an invalid KUBECONFIG
		kubeconfig := "invalid-kubeconfig"
		os.Setenv("KUBECONFIG", kubeconfig)
		defer os.Unsetenv("KUBECONFIG")

		config, err := K8sRestConfig()
		assert.NotNil(t, err)
		assert.Nil(t, config)
	})

}
