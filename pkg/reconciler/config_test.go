package reconciler

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestGlobalConfig_GetDefaultContainerResources(t *testing.T) {
	type fields struct {
		Defaults *DefaultConfig
	}
	tests := []struct {
		name    string
		fields  fields
		want    corev1.ResourceRequirements
		wantErr bool
	}{
		{
			name: "Test GetDefaultContainerResources with empty config",
			fields: fields{
				Defaults: &DefaultConfig{
					ContainerResources: "",
				},
			},
			want: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{},
				Requests: corev1.ResourceList{
					"cpu":    resource.MustParse("100m"),
					"memory": resource.MustParse("128Mi"),
				},
			},
			wantErr: false,
		},
		{
			name: "Test GetDefaultContainerResources",
			fields: fields{
				Defaults: &DefaultConfig{
					ContainerResources: "limits:\n  cpu: 200m\n  memory: 256Mi\nrequests:\n  cpu: 50m\n  memory: 64Mi\n",
				},
			},
			want: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"cpu":    resource.MustParse("200m"),
					"memory": resource.MustParse("256Mi"),
				},
				Requests: corev1.ResourceList{
					"cpu":    resource.MustParse("50m"),
					"memory": resource.MustParse("64Mi"),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		g := &GlobalConfig{
			Defaults: tt.fields.Defaults,
		}
		got := g.GetDefaultContainerResources()
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. GlobalConfig.GetDefaultContainerResources() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
