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

// IDTokenClaims is the claims extract from the IDToken.
type IDTokenClaims struct {
	Iss               string   `json:"iss"`
	Sub               string   `json:"sub"`
	Aud               string   `json:"aud"`
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

// UserInfo includes information about the user identity
// It holds the IDTokenClaims, IDToken and RefreshToken for the user
type UserInfo struct {
	IDTokenClaims IDTokenClaims `json:"id_token_claims"`
	IDToken       string        `json:"id_token"`
	RefreshToken  string        `json:"refresh_token"`
}

func NewUserInfo(itc IDTokenClaims, idToken string, refreshToken string) UserInfo {
	return UserInfo{
		IDTokenClaims: itc,
		IDToken:       idToken,
		RefreshToken:  refreshToken,
	}
}
