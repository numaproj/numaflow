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

import "encoding/json"

// IDTokenClaims is the claims extract from the IDToken.
type IDTokenClaims struct {
	Iss               string   `json:"iss"`
	Sub               string   `json:"sub"`
	Aud               Audience `json:"aud"`
	Exp               int      `json:"exp"`
	Iat               int      `json:"iat"`
	AtHash            string   `json:"at_hash"`
	CHash             string   `json:"c_hash"`
	Email             string   `json:"email"`
	EmailVerified     bool     `json:"email_verified"`
	Groups            []string `json:"groups"`
	Name              string   `json:"name"`
	PreferredUsername string   `json:"preferred_username"`
}

// Audience is the "aud" claim. OIDC allows a single string or an array of
// strings, and Dex sends an array for tokens issued to a trusted peer.
type Audience []string

func (a *Audience) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		*a = Audience{s}
		return nil
	}
	var auds []string
	if err := json.Unmarshal(b, &auds); err != nil {
		return err
	}
	*a = auds
	return nil
}

// MarshalJSON writes no audience or a single one as a string, as before, so
// the claims sent to the UI keep their shape.
func (a Audience) MarshalJSON() ([]byte, error) {
	switch len(a) {
	case 0:
		return json.Marshal("")
	case 1:
		return json.Marshal(a[0])
	}
	return json.Marshal([]string(a))
}

// UserInfo includes information about the user identity
// It holds the IDTokenClaims, IDToken and RefreshToken for the user
type UserInfo struct {
	IDTokenClaims *IDTokenClaims `json:"id_token_claims,omitempty"`
	IDToken       string         `json:"id_token"`
	RefreshToken  string         `json:"refresh_token"`
}

func NewUserInfo(itc *IDTokenClaims, idToken string, refreshToken string) UserInfo {
	return UserInfo{
		IDTokenClaims: itc,
		IDToken:       idToken,
		RefreshToken:  refreshToken,
	}
}
