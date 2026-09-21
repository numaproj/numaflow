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

import "fmt"

type Service struct {
	mode Mode
}

// NewService creates the Pod View capability service for the configured rollout mode.
// An omitted mode is treated as disabled so new deployments retain the classic experience.
func NewService(mode Mode) (*Service, error) {
	if mode == "" {
		mode = ModeDisabled
	}
	if !mode.Valid() {
		return nil, fmt.Errorf("unsupported Pod View v2 mode %q", mode)
	}
	return &Service{mode: mode}, nil
}

// Valid reports whether m is a rollout mode supported by the API v2 contract.
func (m Mode) Valid() bool {
	switch m {
	case ModeDisabled, ModeOptIn, ModeDefault, ModeRequired:
		return true
	default:
		return false
	}
}
