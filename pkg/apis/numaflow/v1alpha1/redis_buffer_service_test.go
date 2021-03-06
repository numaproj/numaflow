package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisGetStatefulSetSpec(t *testing.T) {
	req := GetRedisStatefulSetSpecReq{
		ServiceName:               "test-svc-name",
		Labels:                    map[string]string{"a": "b"},
		RedisImage:                "test-redis-image",
		SentinelImage:             "test-s-image",
		MetricsExporterImage:      "test-m-image",
		RedisContainerPort:        1234,
		SentinelContainerPort:     3333,
		RedisMetricsContainerPort: 4321,
		PvcNameIfNeeded:           "test-pvc",
		TLSEnabled:                false,
		CredentialSecretName:      "test-secret",
		ConfConfigMapName:         "test-c1",
		ScriptsConfigMapName:      "test-c2",
		HealthConfigMapName:       "test-c3",
	}
	t.Run("without persistence", func(t *testing.T) {
		s := &NativeRedis{}
		spec := s.GetStatefulSetSpec(req)
		assert.Equal(t, int32(3), *spec.Replicas)
		assert.Equal(t, "test-svc-name", spec.ServiceName)
		assert.Equal(t, "test-redis-image", spec.Template.Spec.Containers[0].Image)
		assert.Equal(t, "test-s-image", spec.Template.Spec.Containers[1].Image)
		assert.Equal(t, "test-m-image", spec.Template.Spec.Containers[2].Image)
		assert.Equal(t, "b", spec.Selector.MatchLabels["a"])
		assert.Equal(t, 3, int(*spec.Replicas))
		assert.Equal(t, int32(1234), spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
		assert.Equal(t, int32(3333), spec.Template.Spec.Containers[1].Ports[0].ContainerPort)
		assert.Equal(t, int32(4321), spec.Template.Spec.Containers[2].Ports[0].ContainerPort)
		assert.False(t, len(spec.VolumeClaimTemplates) > 0)
		assert.True(t, len(spec.Template.Spec.Volumes) > 0)
	})

	t.Run("with persistence", func(t *testing.T) {
		st := "test"
		s := &NativeRedis{
			Persistence: &PersistenceStrategy{
				StorageClassName: &st,
			},
		}
		spec := s.GetStatefulSetSpec(req)
		assert.True(t, len(spec.VolumeClaimTemplates) > 0)
		assert.True(t, len(spec.Template.Spec.InitContainers) > 0)
		assert.NotNil(t, spec.Template.Spec.SecurityContext)
		assert.NotNil(t, spec.Template.Spec.Containers[0].SecurityContext)
		assert.NotNil(t, spec.Template.Spec.Containers[1].SecurityContext)
		assert.NotNil(t, spec.Template.Spec.SecurityContext.FSGroup)
	})
}

func TestRedisGetServiceSpec(t *testing.T) {
	s := NativeRedis{}
	spec := s.GetServiceSpec(GetRedisServiceSpecReq{
		Labels:                map[string]string{"a": "b"},
		RedisContainerPort:    1234,
		SentinelContainerPort: 4321,
	})
	assert.Equal(t, 2, len(spec.Ports))
}

func TestRedisGetHeadlessServiceSpec(t *testing.T) {
	s := NativeRedis{}
	spec := s.GetHeadlessServiceSpec(GetRedisServiceSpecReq{
		Labels:                map[string]string{"a": "b"},
		RedisContainerPort:    1234,
		SentinelContainerPort: 4321,
	})
	assert.Equal(t, 2, len(spec.Ports))
}

func Test_RedisBufferGetReplicas(t *testing.T) {
	s := NativeRedis{}
	assert.Equal(t, 3, s.GetReplicas())
	five := int32(5)
	s.Replicas = &five
	assert.Equal(t, 5, s.GetReplicas())
	two := int32(2)
	s.Replicas = &two
	assert.Equal(t, 3, s.GetReplicas())
}
