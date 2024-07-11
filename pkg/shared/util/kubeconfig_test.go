package util

import (
	"os"
	"path/filepath"
	"testing"

	"k8s.io/client-go/util/homedir"

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

func TestK8sRestConfig_blank(t *testing.T) {
	os.Unsetenv("KUBECONFIG")

	// Ensure the default kubeconfig does not exist
	homeDir := homedir.HomeDir()
	defaultKubeconfigPath := filepath.Join(homeDir, ".kube", "config")
	os.Remove(defaultKubeconfigPath)

	restConfig, err := K8sRestConfig()
	assert.Error(t, err)
	assert.Nil(t, restConfig)
}
