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

// GetCapabilities describes the API v2 features and Pod View experience available
// from this server. It advertises only operations that are mounted in this release.
func (s *Service) GetCapabilities() Capabilities {
	podView := Capability{
		Mode:                 s.mode,
		Eligible:             true,
		DefaultExperience:    ExperienceClassic,
		AllowClassicFallback: true,
	}
	switch s.mode {
	case ModeDefault:
		podView.DefaultExperience = ExperienceNext
	case ModeRequired:
		podView.DefaultExperience = ExperienceNext
		podView.AllowClassicFallback = false
	}

	return Capabilities{
		APIVersion: "v2",
		PodView:    podView,
		Operations: []string{"getCapabilities"},
		Limits: Limits{
			DefaultPageSize:     50,
			MaximumPageSize:     200,
			MaximumLogLines:     1000,
			MaximumMetricPoints: 2000,
		},
	}
}
