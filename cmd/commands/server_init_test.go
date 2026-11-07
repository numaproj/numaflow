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

package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuntimeConfigJS(t *testing.T) {
	tests := []struct {
		name     string
		baseHref string
		label    string
		icon     string
		want     string
	}{
		{
			name:     "defaults",
			baseHref: "/",
			want:     `window.__RUNTIME_CONFIG__ = {"BASE_HREF":"/"};`,
		},
		{
			name:     "label and icon",
			baseHref: "/numaflow",
			label:    "Login with Keycloak",
			icon:     "none",
			want:     `window.__RUNTIME_CONFIG__ = {"BASE_HREF":"/numaflow","LOGIN_SSO_LABEL":"Login with Keycloak","LOGIN_SSO_ICON":"none"};`,
		},
		{
			name:     "escaped label",
			baseHref: "/",
			label:    `Say "hi" </script>`,
			icon:     "https://example.com/logo.png",
			want:     `window.__RUNTIME_CONFIG__ = {"BASE_HREF":"/","LOGIN_SSO_LABEL":"Say \"hi\" \u003c/script\u003e","LOGIN_SSO_ICON":"https://example.com/logo.png"};`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runtimeConfigJS(tt.baseHref, tt.label, tt.icon)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
