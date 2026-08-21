/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

const cmdParamsConfigMapName = "numaflow-cmd-params-config"

func TestParseImageVersion(t *testing.T) {
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{name: "tagged image", image: "quay.io/numaproj/numaflow:v1.7.5", want: "v1.7.5"},
		{name: "latest tag", image: "docker.intuit.com/quay-rmt/numaproj/numaflow:latest", want: "latest"},
		{name: "tag with digest", image: "quay.io/numaproj/numaflow:v1.7.5@sha256:abc", want: "v1.7.5"},
		// Digest-only refs have no tag; returning a digest fragment would be misleading.
		{name: "digest only", image: "quay.io/numaproj/numaflow@sha256:abcdef", want: ""},
		{name: "no tag", image: "quay.io/numaproj/numaflow", want: ""},
		{name: "empty", image: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseImageVersion(tt.image))
		})
	}
}

func TestControllerScopeFromPodSpec(t *testing.T) {
	t.Run("args override env", func(t *testing.T) {
		// CLI flags take precedence over ConfigMap-backed env, matching the controller binary.
		spec := corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: controllerContainerName,
				Args: []string{"controller", "--namespaced", "--managed-namespace", "from-args"},
				Env: []corev1.EnvVar{
					{Name: envControllerNamespaced, Value: "false"},
					{Name: envControllerManagedNamespace, Value: "from-env"},
				},
			}},
		}
		namespaced, managed := controllerScopeFromPodSpec(spec, nil)
		assert.True(t, namespaced)
		assert.Equal(t, "from-args", managed)
	})

	t.Run("env when args absent", func(t *testing.T) {
		spec := corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: controllerContainerName,
				Args: []string{"controller"},
				Env: []corev1.EnvVar{
					{Name: envControllerNamespaced, Value: "true"},
					{Name: envControllerManagedNamespace, Value: "managed-ns"},
				},
			}},
		}
		namespaced, managed := controllerScopeFromPodSpec(spec, nil)
		assert.True(t, namespaced)
		assert.Equal(t, "managed-ns", managed)
	})

	t.Run("equals-form args", func(t *testing.T) {
		spec := corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: controllerContainerName,
				Args: []string{"controller", "--namespaced=true", "--managed-namespace=eq-ns"},
			}},
		}
		namespaced, managed := controllerScopeFromPodSpec(spec, nil)
		assert.True(t, namespaced)
		assert.Equal(t, "eq-ns", managed)
	})
}

func TestExtractControllerInfo(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: controllerDeploymentName},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  controllerContainerName,
						Image: "quay.io/numaproj/numaflow:v1.7.5",
						Args:  []string{"controller", "--namespaced"},
					}},
				},
			},
		},
	}
	info := extractControllerInfo("app-ns", dep, nil)
	assert.True(t, info.Found)
	assert.Equal(t, "app-ns", info.Namespace)
	assert.Equal(t, controllerDeploymentName, info.Name)
	assert.Equal(t, "v1.7.5", info.Version)
	assert.Equal(t, "quay.io/numaproj/numaflow:v1.7.5", info.Image)
	assert.True(t, info.Namespaced)
	assert.Equal(t, "app-ns", info.ManagedNamespace)
	require.NotNil(t, info.Replicas)
	assert.Equal(t, int32(1), *info.Replicas)
}

func TestGetControllerInfo_FoundByName(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dep := fakeControllerDeployment("numaflow-system", controllerDeploymentName, "quay.io/numaproj/numaflow:v1.4.0", nil, nil)
	h := &handler{kubeClient: fake.NewSimpleClientset(dep)}

	c, w := newControllerInfoContext("numaflow-system")
	h.GetControllerInfo(c)

	require.Equal(t, http.StatusOK, w.Code)
	got := decodeControllerInfo(t, w)
	assert.Nil(t, got.ErrMsg)
	assert.True(t, got.Data.Found)
	assert.Equal(t, "v1.4.0", got.Data.Version)
	assert.False(t, got.Data.Namespaced)
}

func TestGetControllerInfo_FoundByTemplateLabel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// Renamed Deployment still identifiable via pod-template component label (no metadata labels).
	dep := fakeControllerDeployment("app-ns", "custom-controller", "quay.io/numaproj/numaflow:v1.7.5",
		[]string{"controller", "--namespaced", "--managed-namespace", "app-ns"}, nil)
	h := &handler{kubeClient: fake.NewSimpleClientset(dep)}

	c, w := newControllerInfoContext("app-ns")
	h.GetControllerInfo(c)

	require.Equal(t, http.StatusOK, w.Code)
	got := decodeControllerInfo(t, w)
	assert.True(t, got.Data.Found)
	assert.Equal(t, "custom-controller", got.Data.Name)
	assert.True(t, got.Data.Namespaced)
	assert.Equal(t, "app-ns", got.Data.ManagedNamespace)
}

func TestGetControllerInfo_FoundByMetadataLabel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dep := fakeControllerDeployment("app-ns", "custom-controller", "quay.io/numaproj/numaflow:v1.7.5", nil, nil)
	dep.Labels = map[string]string{dfv1.KeyComponent: dfv1.ComponentControllerManager}
	h := &handler{kubeClient: fake.NewSimpleClientset(dep)}

	c, w := newControllerInfoContext("app-ns")
	h.GetControllerInfo(c)

	require.Equal(t, http.StatusOK, w.Code)
	got := decodeControllerInfo(t, w)
	assert.True(t, got.Data.Found)
	assert.Equal(t, "custom-controller", got.Data.Name)
}

func TestGetControllerInfo_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &handler{kubeClient: fake.NewSimpleClientset()}

	c, w := newControllerInfoContext("empty-ns")
	h.GetControllerInfo(c)

	require.Equal(t, http.StatusOK, w.Code)
	got := decodeControllerInfo(t, w)
	assert.Nil(t, got.ErrMsg)
	assert.False(t, got.Data.Found)
	assert.Equal(t, "empty-ns", got.Data.Namespace)
}

func TestGetControllerInfo_ScopeFromConfigMapKeyRef(t *testing.T) {
	gin.SetMode(gin.TestMode)
	optional := true
	dep := fakeControllerDeployment("app-ns", controllerDeploymentName, "quay.io/numaproj/numaflow:v1.8.1",
		[]string{"controller"},
		[]corev1.EnvVar{
			{
				Name: envControllerNamespaced,
				ValueFrom: &corev1.EnvVarSource{
					ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: cmdParamsConfigMapName},
						Key:                  "namespaced",
						Optional:             &optional,
					},
				},
			},
			{
				Name: envControllerManagedNamespace,
				ValueFrom: &corev1.EnvVarSource{
					ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: cmdParamsConfigMapName},
						Key:                  "managed.namespace",
						Optional:             &optional,
					},
				},
			},
		},
	)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmdParamsConfigMapName, Namespace: "app-ns"},
		Data: map[string]string{
			"namespaced":        "true",
			"managed.namespace": "app-ns",
		},
	}
	h := &handler{kubeClient: fake.NewSimpleClientset(dep, cm)}

	c, w := newControllerInfoContext("app-ns")
	h.GetControllerInfo(c)

	require.Equal(t, http.StatusOK, w.Code)
	got := decodeControllerInfo(t, w)
	assert.True(t, got.Data.Found)
	assert.True(t, got.Data.Namespaced)
	assert.Equal(t, "app-ns", got.Data.ManagedNamespace)
	assert.Equal(t, "v1.8.1", got.Data.Version)
}

func TestGetControllerInfo_OptionalConfigMapMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	optional := true
	dep := fakeControllerDeployment("numaflow-system", controllerDeploymentName, "quay.io/numaproj/numaflow:v1.8.1",
		[]string{"controller"},
		[]corev1.EnvVar{
			{
				Name: envControllerNamespaced,
				ValueFrom: &corev1.EnvVarSource{
					ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: cmdParamsConfigMapName},
						Key:                  "namespaced",
						Optional:             &optional,
					},
				},
			},
		},
	)
	// ConfigMap absent (optional) → cluster-scoped defaults.
	h := &handler{kubeClient: fake.NewSimpleClientset(dep)}

	c, w := newControllerInfoContext("numaflow-system")
	h.GetControllerInfo(c)

	require.Equal(t, http.StatusOK, w.Code)
	got := decodeControllerInfo(t, w)
	assert.True(t, got.Data.Found)
	assert.False(t, got.Data.Namespaced)
	assert.Empty(t, got.Data.ManagedNamespace)
}

func TestGetControllerInfo_ArgsOverrideConfigMapKeyRef(t *testing.T) {
	gin.SetMode(gin.TestMode)
	optional := true
	dep := fakeControllerDeployment("app-ns", controllerDeploymentName, "quay.io/numaproj/numaflow:v1.8.1",
		[]string{"controller", "--namespaced", "--managed-namespace", "from-args"},
		[]corev1.EnvVar{
			{
				Name: envControllerNamespaced,
				ValueFrom: &corev1.EnvVarSource{
					ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: cmdParamsConfigMapName},
						Key:                  "namespaced",
						Optional:             &optional,
					},
				},
			},
			{
				Name: envControllerManagedNamespace,
				ValueFrom: &corev1.EnvVarSource{
					ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: cmdParamsConfigMapName},
						Key:                  "managed.namespace",
						Optional:             &optional,
					},
				},
			},
		},
	)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmdParamsConfigMapName, Namespace: "app-ns"},
		Data: map[string]string{
			"namespaced":        "false",
			"managed.namespace": "from-cm",
		},
	}
	h := &handler{kubeClient: fake.NewSimpleClientset(dep, cm)}

	c, w := newControllerInfoContext("app-ns")
	h.GetControllerInfo(c)

	require.Equal(t, http.StatusOK, w.Code)
	got := decodeControllerInfo(t, w)
	assert.True(t, got.Data.Namespaced)
	assert.Equal(t, "from-args", got.Data.ManagedNamespace)
}

func fakeControllerDeployment(namespace, name, image string, args []string, env []corev1.EnvVar) *appsv1.Deployment {
	if args == nil {
		args = []string{"controller"}
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					dfv1.KeyComponent: dfv1.ComponentControllerManager,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						dfv1.KeyComponent: dfv1.ComponentControllerManager,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  controllerContainerName,
						Image: image,
						Args:  args,
						Env:   env,
					}},
				},
			},
		},
	}
}

func newControllerInfoContext(namespace string) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/"+namespace+"/controller-info", nil)
	c.Params = gin.Params{{Key: "namespace", Value: namespace}}
	return c, w
}

func decodeControllerInfo(t *testing.T, w *httptest.ResponseRecorder) struct {
	ErrMsg *string        `json:"errMsg"`
	Data   ControllerInfo `json:"data"`
} {
	t.Helper()
	var got struct {
		ErrMsg *string        `json:"errMsg"`
		Data   ControllerInfo `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	return got
}
