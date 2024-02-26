package util

import (
	"testing"

	"github.com/IBM/sarama"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestSaslConfiguration(t *testing.T) {
	mockedVolumes := MockedVolumes{
		volumeSecrets: map[struct {
			objectName string
			key        string
		}]string{
			{
				objectName: "user-secret-name",
				key:        "user",
			}: "user",
			{
				objectName: "password-secret-name",
				key:        "password",
			}: "password",
		},
	}

	credentials := &dfv1.SASLPlain{
		UserSecret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: "user-secret-name",
			},
			Key: "user",
		},
		PasswordSecret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: "password-secret-name",
			},
			Key: "password",
		},
		Handshake: true,
	}

	t.Run("Plain produces right values", func(t *testing.T) {
		plain := dfv1.SASLTypePlaintext
		config, err := GetSASLStrategy(&dfv1.SASL{
			Mechanism: &plain,
			Plain:     credentials,
		}, mockedVolumes)
		assert.NoError(t, err)
		assert.Equal(t, true, config.Enable)
		assert.Equal(t, sarama.SASLTypePlaintext, string(config.Mechanism))
		assert.Equal(t, true, config.Handshake)
		assert.Equal(t, "user", config.User)
		assert.Equal(t, "password", config.Password)
	})

	t.Run("SCRAM SHA 256 produces right values", func(t *testing.T) {
		sasl_256 := dfv1.SASLTypeSCRAMSHA256
		config, err := GetSASLStrategy(&dfv1.SASL{
			Mechanism:   &sasl_256,
			SCRAMSHA256: credentials,
		}, mockedVolumes)
		assert.NoError(t, err)
		assert.Equal(t, true, config.Enable)
		assert.Equal(t, sarama.SASLTypeSCRAMSHA256, string(config.Mechanism))
		assert.Equal(t, true, config.Handshake)
		assert.Equal(t, "user", config.User)
		assert.Equal(t, "password", config.Password)
	})

	t.Run("SCRAM SHA 512 produces right values", func(t *testing.T) {
		sasl_512 := dfv1.SASLTypeSCRAMSHA512
		config, err := GetSASLStrategy(&dfv1.SASL{
			Mechanism:   &sasl_512,
			SCRAMSHA512: credentials,
		}, mockedVolumes)
		assert.NoError(t, err)
		assert.Equal(t, true, config.Enable)
		assert.Equal(t, sarama.SASLTypeSCRAMSHA512, string(config.Mechanism))
		assert.Equal(t, true, config.Handshake)
		assert.Equal(t, "user", config.User)
		assert.Equal(t, "password", config.Password)
	})
}
