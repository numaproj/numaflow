/*
Copyright 2026 The Numaproj Authors.

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

package podview

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapabilities(t *testing.T) {
	tests := []struct {
		mode            Mode
		defaultView     Experience
		classicFallback bool
	}{
		{ModeDisabled, ExperienceClassic, true},
		{ModeOptIn, ExperienceClassic, true},
		{ModeDefault, ExperienceNext, true},
		{ModeRequired, ExperienceNext, false},
	}

	for _, test := range tests {
		t.Run(string(test.mode), func(t *testing.T) {
			service, err := NewService(test.mode)
			require.NoError(t, err)

			capabilities := service.GetCapabilities()
			assert.Equal(t, "v2", capabilities.APIVersion)
			assert.Equal(t, test.mode, capabilities.PodView.Mode)
			assert.True(t, capabilities.PodView.Eligible)
			assert.Equal(t, test.defaultView, capabilities.PodView.DefaultExperience)
			assert.Equal(t, test.classicFallback, capabilities.PodView.AllowClassicFallback)
			assert.Equal(t, []string{"getCapabilities"}, capabilities.Operations)
			assert.Equal(t, Limits{
				DefaultPageSize:     50,
				MaximumPageSize:     200,
				MaximumLogLines:     1000,
				MaximumMetricPoints: 2000,
			}, capabilities.Limits)
		})
	}
}

func TestNewServiceUsesDisabledModeByDefault(t *testing.T) {
	service, err := NewService("")
	require.NoError(t, err)
	assert.Equal(t, ModeDisabled, service.GetCapabilities().PodView.Mode)
}

func TestNewServiceRejectsUnsupportedMode(t *testing.T) {
	_, err := NewService(Mode("invalid"))
	require.Error(t, err)
}
