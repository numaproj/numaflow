package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
)

func Test_Sink_getContainers(t *testing.T) {
	s := Sink{}
	c, err := s.getContainers(getContainerReq{
		env: []corev1.EnvVar{
			{Name: "test-env", Value: "test-val"},
		},
		isbSvcType:      ISBSvcTypeJetStream,
		imagePullPolicy: corev1.PullIfNotPresent,
		image:           testFlowImage,
		resources:       corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(c))
	assert.Equal(t, testFlowImage, c[0].Image)
	assert.Equal(t, corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}}, c[0].Resources)
}
