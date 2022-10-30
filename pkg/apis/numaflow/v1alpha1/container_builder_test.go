package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
)

var (
	testResources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			"cpu":    resource.MustParse("800m"),
			"memory": resource.MustParse("256Mi"),
		},
		Requests: corev1.ResourceList{
			"cpu":    resource.MustParse("100m"),
			"memory": resource.MustParse("64Mi"),
		},
	}
)

func Test_containerBuilder(t *testing.T) {
	c := containerBuilder{}.
		init(getContainerReq{
			resources: testResources,
		}).
		build()
	assert.Equal(t, "numa", c.Name)
	assert.Len(t, c.VolumeMounts, 0)
	assert.Equal(t, testResources, c.Resources)
}
