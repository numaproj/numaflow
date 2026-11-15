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

package authn

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIDTokenClaims_Audience(t *testing.T) {
	tests := []struct {
		name string
		aud  string
		want Audience
	}{
		{name: "string", aud: `"numaflow-server-app"`, want: Audience{"numaflow-server-app"}},
		{name: "array from a trusted peer", aud: `["numaflow-mmi","numaflow-server-app"]`, want: Audience{"numaflow-mmi", "numaflow-server-app"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := []byte(`{"iss":"https://dex.example.com","sub":"user-1","aud":` + tt.aud +
				`,"exp":1900000000,"iat":1800000000,"email":"user@example.com","groups":["admins"]}`)
			var claims IDTokenClaims
			require.NoError(t, json.Unmarshal(payload, &claims))
			assert.Equal(t, tt.want, claims.Aud)
			assert.Equal(t, "user@example.com", claims.Email)
			assert.Equal(t, []string{"admins"}, claims.Groups)

			out, err := json.Marshal(claims)
			require.NoError(t, err)
			var back map[string]any
			require.NoError(t, json.Unmarshal(out, &back))
			assert.JSONEq(t, tt.aud, mustJSON(t, back["aud"]))
		})
	}
}

// Local users have no audience; their claims must still marshal "aud" as "".
func TestIDTokenClaims_NoAudience(t *testing.T) {
	out, err := json.Marshal(IDTokenClaims{Iss: "numaflow", Name: "admin"})
	require.NoError(t, err)
	var back map[string]any
	require.NoError(t, json.Unmarshal(out, &back))
	assert.Equal(t, "", back["aud"])
}

func TestIDTokenClaims_AudienceInvalid(t *testing.T) {
	var claims IDTokenClaims
	assert.Error(t, json.Unmarshal([]byte(`{"aud":42}`), &claims))
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}
