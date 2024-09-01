package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

func TestUpdateStrategy_GetUpdateStrategyType(t *testing.T) {
	tests := []struct {
		name     string
		strategy UpdateStrategy
		expected UpdateStrategyType
	}{
		{
			name:     "RollingUpdateStrategyType",
			strategy: UpdateStrategy{Type: RollingUpdateStrategyType},
			expected: RollingUpdateStrategyType,
		},
		{
			name:     "EmptyType",
			strategy: UpdateStrategy{},
			expected: RollingUpdateStrategyType,
		},
		{
			name:     "UnsupportedType",
			strategy: UpdateStrategy{Type: "UnsupportedType"},
			expected: RollingUpdateStrategyType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.strategy.GetUpdateStrategyType()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestUpdateStrategy_GetRollingUpdateStrategy(t *testing.T) {
	tests := []struct {
		name     string
		strategy UpdateStrategy
		expected RollingUpdateStrategy
	}{
		{
			name:     "DefaultStrategy",
			strategy: UpdateStrategy{},
			expected: RollingUpdateStrategy{
				MaxUnavailable: ptr.To[intstr.IntOrString](intstr.FromString("25%")),
			},
		},
		{
			name: "CustomMaxUnavailable",
			strategy: UpdateStrategy{
				RollingUpdate: &RollingUpdateStrategy{
					MaxUnavailable: ptr.To[intstr.IntOrString](intstr.FromInt(5)),
				},
			},
			expected: RollingUpdateStrategy{
				MaxUnavailable: ptr.To[intstr.IntOrString](intstr.FromInt(5)),
			},
		},
		{
			name: "NilRollingUpdate",
			strategy: UpdateStrategy{
				RollingUpdate: nil,
			},
			expected: RollingUpdateStrategy{
				MaxUnavailable: ptr.To[intstr.IntOrString](intstr.FromString("25%")),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.strategy.GetRollingUpdateStrategy()
			assert.Equal(t, tt.expected, result)
		})
	}
}
