package installer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	statefulSet = &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-statefulset",
			Namespace: "default",
		},
		Status: appsv1.StatefulSetStatus{
			AvailableReplicas:  3,
			CurrentReplicas:    3,
			CurrentRevision:    "isbsvc-default-js-597b7f74d7",
			ObservedGeneration: 1,
			ReadyReplicas:      3,
			Replicas:           3,
			UpdateRevision:     "isbsvc-default-js-597b7f74d7",
			UpdatedReplicas:    3,
		},
	}
)

func TestGetStatefulSetStatus(t *testing.T) {
	t.Run("Test statefulset status as true", func(t *testing.T) {
		testSts := statefulSet.DeepCopy()
		msg, status := getStatefulSetStatus(testSts)
		assert.True(t, status)
		assert.Equal(t, "statefulset rolling update complete 3 pods at revision isbsvc-default-js-597b7f74d7...\n", msg)
	})

	t.Run("Test statefulset status as false", func(t *testing.T) {
		testSts := statefulSet.DeepCopy()
		testSts.Status.UpdateRevision = "isbsvc-default-js-597b7f73a1"
		msg, status := getStatefulSetStatus(testSts)
		assert.False(t, status)
		assert.Equal(t, "waiting for statefulset rolling update to complete 3 pods at revision isbsvc-default-js-597b7f73a1...\n", msg)
	})

	t.Run("Test statefulset with ObservedGeneration as zero", func(t *testing.T) {
		testSts := statefulSet.DeepCopy()
		testSts.Status.ObservedGeneration = 0
		msg, status := getStatefulSetStatus(testSts)
		assert.False(t, status)
		assert.Equal(t, "Waiting for statefulset spec update to be observed...\n", msg)
	})
}
