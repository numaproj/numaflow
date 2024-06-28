package util

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestK8sRestConfig(t *testing.T) {
	t.Run("K8sRestConfig returns rest.Config", func(t *testing.T) {
		config, err := K8sRestConfig()
		assert.Nil(t, err)
		assert.NotNil(t, config)
	})

	t.Run("K8sRestConfig returns error when KUBECONFIG is invalid", func(t *testing.T) {
		// Setup the environment to simulate an invalid KUBECONFIG
		kubeconfig := "invalid-kubeconfig"
		os.Setenv("KUBECONFIG", kubeconfig)
		defer os.Unsetenv("KUBECONFIG")

		// Mock the clientcmd and os packages if needed for different conditions
		config, err := K8sRestConfig()
		assert.NotNil(t, err)
		assert.Nil(t, config)
	})

	t.Run("K8sRestConfig returns error when ~/.kube/config is invalid", func(t *testing.T) {
		// Unset KUBECONFIG environment variable
		temp := os.Getenv("KUBECONFIG")
		os.Unsetenv("KUBECONFIG")
		defer os.Setenv("KUBECONFIG", temp)

		// Simulate invalid ~/.kube/config
		home, _ := os.UserHomeDir()
		invalidConfigPath := home + "/.kube/config"
		os.Rename(invalidConfigPath, invalidConfigPath+".bak")
		defer os.Rename(invalidConfigPath+".bak", invalidConfigPath)

		config, err := K8sRestConfig()
		assert.NotNil(t, err)
		assert.Nil(t, config)

	})

}
