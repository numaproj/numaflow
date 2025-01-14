package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	intstr "k8s.io/apimachinery/pkg/util/intstr"
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
			name:     "NilRollingUpdate",
			strategy: UpdateStrategy{},
			expected: RollingUpdateStrategy{},
		},
		{
			name: "NonNilRollingUpdate",
			strategy: UpdateStrategy{
				RollingUpdate: &RollingUpdateStrategy{
					MaxUnavailable: ptr.To[intstr.IntOrString](intstr.FromString("2")),
				},
			},
			expected: RollingUpdateStrategy{
				MaxUnavailable: ptr.To[intstr.IntOrString](intstr.FromString("2")),
			},
		},
		{
			name: "EmptyRollingUpdate",
			strategy: UpdateStrategy{
				RollingUpdate: &RollingUpdateStrategy{},
			},
			expected: RollingUpdateStrategy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.strategy.GetRollingUpdateStrategy()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRollingUpdateStrategy_GetMaxUnavailable(t *testing.T) {
	tests := []struct {
		name     string
		strategy RollingUpdateStrategy
		expected intstr.IntOrString
	}{
		{
			name:     "NilMaxUnavailable",
			strategy: RollingUpdateStrategy{},
			expected: intstr.FromString("25%"),
		},
		{
			name: "IntegerMaxUnavailable",
			strategy: RollingUpdateStrategy{
				MaxUnavailable: ptr.To[intstr.IntOrString](intstr.FromInt(5)),
			},
			expected: intstr.FromInt(5),
		},
		{
			name: "StringMaxUnavailable",
			strategy: RollingUpdateStrategy{
				MaxUnavailable: ptr.To[intstr.IntOrString](intstr.FromString("50%")),
			},
			expected: intstr.FromString("50%"),
		},
		{
			name: "ZeroIntegerMaxUnavailable",
			strategy: RollingUpdateStrategy{
				MaxUnavailable: ptr.To[intstr.IntOrString](intstr.FromInt(0)),
			},
			expected: intstr.FromInt(0),
		},
		{
			name: "ZeroPercentMaxUnavailable",
			strategy: RollingUpdateStrategy{
				MaxUnavailable: ptr.To[intstr.IntOrString](intstr.FromString("0%")),
			},
			expected: intstr.FromString("0%"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.strategy.GetMaxUnavailable()
			assert.Equal(t, tt.expected, result)
		})
	}
}
