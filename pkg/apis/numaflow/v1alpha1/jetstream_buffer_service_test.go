package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestJetStreamGetStatefulSetSpec(t *testing.T) {
	req := GetJetStreamStatefulSetSpecReq{
		ServiceName:          "test-svc-name",
		Labels:               map[string]string{"a": "b"},
		NatsImage:            "test-nats-image",
		MetricsExporterImage: "test-m-image",
		ConfigReloaderImage:  "test-c-image",
		MetricsPort:          1234,
		ClusterPort:          3333,
		ClientPort:           4321,
		MonitorPort:          2341,
		PvcNameIfNeeded:      "test-pvc",
		ServerAuthSecretName: "test-s-secret",
		ConfigMapName:        "test-cm",
	}
	t.Run("without persistence", func(t *testing.T) {
		s := &JetStreamBufferService{}
		spec := s.GetStatefulSetSpec(req)
		assert.Equal(t, int32(3), *spec.Replicas)
		assert.Equal(t, "test-svc-name", spec.ServiceName)
		assert.Equal(t, "test-nats-image", spec.Template.Spec.Containers[0].Image)
		assert.Equal(t, "test-c-image", spec.Template.Spec.Containers[1].Image)
		assert.Equal(t, "test-m-image", spec.Template.Spec.Containers[2].Image)
		assert.Equal(t, "b", spec.Selector.MatchLabels["a"])
		assert.Equal(t, 3, int(*spec.Replicas))
		assert.Equal(t, "config-volume", spec.Template.Spec.Volumes[1].Name)
		assert.Equal(t, 1, len(spec.Template.Spec.Volumes[1].VolumeSource.Projected.Sources[1].Secret.Items))
		assert.Equal(t, int32(4321), spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
		assert.Equal(t, int32(3333), spec.Template.Spec.Containers[0].Ports[1].ContainerPort)
		assert.Equal(t, int32(2341), spec.Template.Spec.Containers[0].Ports[2].ContainerPort)
		assert.Equal(t, int32(1234), spec.Template.Spec.Containers[2].Ports[0].ContainerPort)
		assert.False(t, len(spec.VolumeClaimTemplates) > 0)
		assert.True(t, len(spec.Template.Spec.Volumes) > 0)
		envNames := []string{}
		for _, e := range spec.Template.Spec.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		for _, e := range []string{"POD_NAME", "SERVER_NAME", "POD_NAMESPACE", "CLUSTER_ADVERTISE", "JS_KEY"} {
			assert.Contains(t, envNames, e)
		}
	})

	t.Run("with persistence", func(t *testing.T) {
		st := "test"
		s := &JetStreamBufferService{
			Persistence: &PersistenceStrategy{
				StorageClassName: &st,
			},
		}
		spec := s.GetStatefulSetSpec(req)
		assert.True(t, len(spec.VolumeClaimTemplates) > 0)
	})

	t.Run("with tls", func(t *testing.T) {
		s := &JetStreamBufferService{
			TLS: true,
		}
		spec := s.GetStatefulSetSpec(req)
		assert.Equal(t, "config-volume", spec.Template.Spec.Volumes[1].Name)
		assert.Equal(t, 7, len(spec.Template.Spec.Volumes[1].VolumeSource.Projected.Sources[1].Secret.Items))
	})

	t.Run("with container resources", func(t *testing.T) {
		r := corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("100Mi"),
			},
		}
		s := &JetStreamBufferService{
			ContainerTemplate:         &ContainerTemplate{Resources: r},
			ReloaderContainerTemplate: &ContainerTemplate{Resources: r},
			MetricsContainerTemplate:  &ContainerTemplate{Resources: r},
		}
		spec := s.GetStatefulSetSpec(req)
		for _, c := range spec.Template.Spec.Containers {
			assert.Equal(t, c.Resources, r)
		}
	})
}

func TestJetStreamGetServiceSpec(t *testing.T) {
	s := JetStreamBufferService{}
	spec := s.GetServiceSpec(GetJetStreamServiceSpecReq{
		Labels:      map[string]string{"a": "b"},
		MetricsPort: 1234,
		ClusterPort: 3333,
		ClientPort:  4321,
		MonitorPort: 2341,
	})
	assert.Equal(t, 4, len(spec.Ports))
	assert.Equal(t, corev1.ClusterIPNone, spec.ClusterIP)
}

func Test_JSBufferGetReplicas(t *testing.T) {
	s := JetStreamBufferService{}
	assert.Equal(t, 3, s.GetReplicas())
	five := int32(5)
	s.Replicas = &five
	assert.Equal(t, 5, s.GetReplicas())
	two := int32(2)
	s.Replicas = &two
	assert.Equal(t, 3, s.GetReplicas())
}
