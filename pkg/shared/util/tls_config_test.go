package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// Mock structures
type Secret struct {
	Name string
}

func TestGetTLSConfig_NilConfig(t *testing.T) {
	config, err := GetTLSConfig(nil)
	assert.NoError(t, err)
	assert.Nil(t, config)
}

func MockTLSObject() *dfv1.TLS {
	return &dfv1.TLS{
		InsecureSkipVerify: true,
		CACertSecret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "ca-cert-secret"},
			Key:                  "caCert",
		},
		CertSecret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "cert-secret"},
			Key:                  "cert",
		},
		KeySecret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "key-secret"},
			Key:                  "key",
		},
	}
}

type MockSecretVolumePath struct {
	mock.Mock
}

func (m *MockSecretVolumePath) GetSecretVolumePath(secret *corev1.SecretKeySelector) (string, error) {
	args := m.Called(secret)
	return args.String(0), args.Error(1)
}

func TestGetTLSConfig_CertWithoutKey(t *testing.T) {
	mockVolumePath := new(MockSecretVolumePath)
	mockTLS := MockTLSObject()
	mockTLS.KeySecret = nil

	mockVolumePath.On("GetSecretVolumePath", mockTLS.CertSecret).Return("/mocked/path/cert-secret", nil)

	_, err := GetTLSConfig(mockTLS)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid tls config")
}

func TestGetTLSConfig_KeyWithoutCert(t *testing.T) {
	mockVolumePath := new(MockSecretVolumePath)
	mockTLS := MockTLSObject()
	mockTLS.CertSecret = nil

	mockVolumePath.On("GetSecretVolumePath", mockTLS.KeySecret).Return("/mocked/path/key-secret", nil)

	_, err := GetTLSConfig(mockTLS)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid tls config")

}
