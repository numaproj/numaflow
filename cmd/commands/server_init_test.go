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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_runtimeEnvJS(t *testing.T) {
	js, err := runtimeEnvJS("/numaflow", `Login with "Keycloak"</script>`, "none")
	require.NoError(t, err)

	s := string(js)
	require.True(t, strings.HasPrefix(s, "window.__RUNTIME_CONFIG__ = "))
	require.True(t, strings.HasSuffix(s, ";"))
	assert.NotContains(t, s, "</script>")

	var cfg map[string]string
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSuffix(strings.TrimPrefix(s, "window.__RUNTIME_CONFIG__ = "), ";")), &cfg))
	assert.Equal(t, map[string]string{
		"BASE_HREF":       "/numaflow",
		"LOGIN_SSO_LABEL": `Login with "Keycloak"</script>`,
		"LOGIN_SSO_ICON":  "none",
	}, cfg)
}
